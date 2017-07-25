package mode

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gomqtt/packet"
)

const (
	mqttConnectTimeout = time.Second * 10
)

var mqttDialer = &net.Dialer{Timeout: mqttConnectTimeout}

type (
	mqttMsgHandler func(*packet.PublishPacket) error

	mqttSubscription struct {
		topic      string
		msgHandler mqttMsgHandler
	}

	mqttConn struct {
		conn          net.Conn
		stream        *packet.Stream
		dc            *DeviceContext
		packetID      uint16
		subs          map[string]mqttSubscription
		command       chan<- *DeviceCommand
		event         <-chan *DeviceEvent
		keyValue      <-chan *ActionKeyValue
		err           chan error
		doPing        chan time.Duration
		outPacket     chan packet.Packet
		puback        chan *packet.PubackPacket
		pingresp      chan *packet.PingrespPacket
		stopEventProc chan bool
		wgWrite       sync.WaitGroup
		wgRead        sync.WaitGroup
	}
)

func (mc *mqttConn) close() {
	close(mc.stopEventProc) // tell event processor to quit
	close(mc.doPing)        // tell pinger to quit

	mc.wgWrite.Wait() // wait for event processor and pinger to finish

	// Attempt graceful disconnect.
	mc.outPacket <- packet.NewDisconnectPacket()
	close(mc.outPacket)

	mc.wgRead.Wait() // wait for packet reader to finish
}

func (mc *mqttConn) getErrorChan() chan error {
	return mc.err
}

func (mc *mqttConn) ping(timeout time.Duration) {
	mc.doPing <- timeout
}

func (mc *mqttConn) sendPacket(p packet.Packet) error {
	if err := mc.stream.Write(p); err != nil {
		logError("[MQTT] failed to send %s packet: %s", p.Type(), err.Error())
		return err
	}

	if err := mc.stream.Flush(); err != nil {
		logError("[MQTT] failed to flush %s packet: %s", p.Type(), err.Error())
		return err
	}

	return nil
}

func (mc *mqttConn) getPacketID() uint16 {
	mc.packetID += 1
	return mc.packetID
}

func (mc *mqttConn) connect() error {
	logInfo("[MQTT] doing CONNECT handshake...")

	p := packet.NewConnectPacket()
	p.Version = packet.Version311
	p.Username = strconv.FormatUint(mc.dc.DeviceID, 10)
	p.Password = mc.dc.AuthToken
	p.CleanSession = true

	if err := mc.sendPacket(p); err != nil {
		return err
	}

	r, err := mc.stream.Read()
	if err != nil {
		logError("[MQTT] failed to read from stream: %s", err.Error())
		return err
	}

	if r.Type() != packet.CONNACK {
		logError("[MQTT] received unexpected packet %s", r.Type())
		return errors.New("unexpected response")
	}

	ack := r.(*packet.ConnackPacket)
	if ack.ReturnCode != packet.ConnectionAccepted {
		return fmt.Errorf("connection failed: %s", ack.ReturnCode.Error())
	}

	return nil
}

func (mc *mqttConn) subscribe(topic string, msgHandler mqttMsgHandler) error {
	logInfo("[MQTT] subscribing to topic %s", topic)

	subs := []packet.Subscription{
		packet.Subscription{
			Topic: topic,
			QOS:   packet.QOSAtLeastOnce, // MODE only supports QoS0 for subscriptions
		},
	}

	p := packet.NewSubscribePacket()
	p.PacketID = mc.getPacketID()
	p.Subscriptions = subs

	if err := mc.sendPacket(p); err != nil {
		return err
	}

	r, err := mc.stream.Read()
	if err != nil {
		logError("[MQTT] failed to read from stream: %s", err.Error())
		return err
	}

	if r.Type() != packet.SUBACK {
		logError("[MQTT] received unexpected packet %s", r.Type())
		return errors.New("unexpected response")
	}

	ack := r.(*packet.SubackPacket)

	if ack.PacketID != p.PacketID {
		logError("[MQTT] received SUBACK packet with wrong packet ID")
		return errors.New("mismatch packet id")
	}

	if len(ack.ReturnCodes) != 1 {
		logError("[MQTT] received SUBACK packet with no return codes")
		return errors.New("invalid packet")
	}

	if ack.ReturnCodes[0] == packet.QOSFailure {
		logError("[MQTT] subscription rejected")
		return errors.New("subscription rejected")
	}

	logInfo("[MQTT] subscription succeeded with QOS %v", ack.ReturnCodes[0])
	mc.subs[topic] = mqttSubscription{topic: topic, msgHandler: msgHandler}
	return nil
}

func (mc *mqttConn) handleCommandMsg(p *packet.PublishPacket) error {
	var cmd struct {
		Action     string                 `json:"action"`
		Parameters map[string]interface{} `json:"parameters"`
	}

	if err := decodeOpaqueJSON(p.Message.Payload, &cmd); err != nil {
		return fmt.Errorf("message data is not valid command JSON: %s", err.Error())
	}

	if cmd.Action == "" {
		return errors.New("message data is not valid command JSON: no action field")
	}

	// Re-encode parameters into JSON payload for later use.
	var payload []byte
	if cmd.Parameters != nil {
		payload, _ = json.Marshal(cmd.Parameters)
	}

	mc.command <- &DeviceCommand{Action: cmd.Action, payload: payload}
	return nil
}

func (mc *mqttConn) handlePublishPacket(p *packet.PublishPacket) {
	sub, exists := mc.subs[p.Message.Topic]
	if !exists {
		logError("[MQTT] received message for invalid topic %s", p.Message.Topic)
		return
	}

	logInfo("[MQTT] received message for topic %s", p.Message.Topic)

	if err := sub.msgHandler(p); err != nil {
		logError("[MQTT] failed to process message: %s", err.Error())
		return
	}
}

func (mc *mqttConn) runPacketReader() {
	logInfo("[MQTT] packet reader is running")
	mc.wgRead.Add(1)

	defer func() {
		logInfo("[MQTT] packet reader is exiting")
		mc.wgRead.Done()
	}()

	for {
		p, err := mc.stream.Read()
		if err != nil {
			mc.err <- err
			break
		}

		switch p.Type() {
		case packet.PUBLISH:
			mc.handlePublishPacket(p.(*packet.PublishPacket))

		case packet.PUBACK:
			mc.puback <- p.(*packet.PubackPacket)

		case packet.PINGRESP:
			mc.pingresp <- p.(*packet.PingrespPacket)

		default:
			logError("[MQTT] received unhandled packet %v", p)
		}
	}
}

func (mc *mqttConn) runPacketWriter() {
	logInfo("[MQTT] packet writer is running")
	defer func() {
		logInfo("[MQTT] packet writer is exiting")
		mc.conn.Close() // this will cause packet reader to exit
	}()

	for p := range mc.outPacket {
		err := mc.sendPacket(p)
		if err != nil {
			logError("[MQTT] failed to send %s packet: %s", p.Type(), err.Error())
			mc.err <- err
			break
		}

		logInfo("[MQTT] successfully sent %s packet", p.Type())
	}
}

func (mc *mqttConn) runPinger() {
	logInfo("[MQTT] pinger is running")
	mc.wgWrite.Add(1)

	defer func() {
		logInfo("[MQTT] pinger is exiting")
		mc.wgWrite.Done()
	}()

	for pingTimeout := range mc.doPing {
		mc.outPacket <- packet.NewPingreqPacket()

		logInfo("[MQTT] waiting for PINGRESP packet")

		select {
		case <-time.After(pingTimeout):
			logError("[MQTT] did not receive PINGRESP packet within %v", pingTimeout)
			mc.err <- errors.New("ping timeout")

		case <-mc.pingresp:
			logInfo("[MQTT] received PINGRESP packet")
		}
	}
}

func (mc *mqttConn) runUpstreamProcessor() {
	logInfo("[MQTT] event processor is running")
	mc.wgWrite.Add(1)

	defer func() {
		logInfo("[MQTT] event processor is exiting")
		mc.wgWrite.Done()
	}()

	for {
		select {
		case <-mc.stopEventProc:
			return

		case e := <-mc.event:
			if err := mc.sendEvent(e); err != nil {
				logError("[MQTT] failed to send event: %s", err.Error())
			}
		case kv := <-mc.keyValue:
			if err := mc.setKeyValue(kv); err != nil {
				logError("[MQTT] failed to set keyValue: %s", err.Error())
			}
		}
	}
}

func (mc *mqttConn) sendEvent(e *DeviceEvent) error {
	var qos byte

	switch e.qos {
	case QOSAtMostOnce:
		qos = packet.QOSAtMostOnce
	case QOSAtLeastOnce:
		qos = packet.QOSAtLeastOnce
	default:
		return errors.New("unsupported qos level")
	}

	payload, _ := json.Marshal(e)

	p := packet.NewPublishPacket()
	p.PacketID = mc.getPacketID()
	p.Message = packet.Message{
		Topic:   fmt.Sprintf("/devices/%d/event", mc.dc.DeviceID),
		QOS:     qos,
		Payload: payload,
	}

	for count := uint(1); count <= maxDeviceEventAttempts; count++ {
		mc.outPacket <- p

		if e.qos == QOSAtMostOnce {
			return nil
		}

		logInfo("[MQTT] event delivery attempt #%d for packet ID %d", count, p.PacketID)
		logInfo("[MQTT] waiting for PUBACK for packet ID %d", p.PacketID)

		select {
		case <-time.After(deviceEventRetryInterval):
			logError("[MQTT] did not receive PUBACK for packet ID %d within %v", p.PacketID, deviceEventRetryInterval)

		case ack := <-mc.puback:
			if ack.PacketID == p.PacketID {
				logInfo("[MQTT] received PUBACK for packet ID %d", ack.PacketID)
				return nil
			}
			// TBD: Something is really wrong if packet ID does not match. What to do?
		}

		p.Dup = true
	}

	return errors.New("event dropped")
}

func (mc *mqttConn) setKeyValue(akv *ActionKeyValue) error {
	qos := packet.QOSAtLeastOnce

	payload, _ := json.Marshal(akv)
	p := packet.NewPublishPacket()
	p.PacketID = mc.getPacketID()
	p.Message = packet.Message{
		Topic:   fmt.Sprintf("/devices/%d/kv", mc.dc.DeviceID),
		QOS:     qos,
		Payload: payload,
	}

	for count := uint(1); count <= maxDeviceKeyValueAttempts; count++ {
		mc.outPacket <- p

		logInfo("[MQTT] key value delivery attempt #%d for packet ID %d", count, p.PacketID)
		logInfo("[MQTT] waiting for PUBACK for packet ID %d", p.PacketID)

		select {
		case <-time.After(deviceKeyValueRetryInterval):
			logError("[MQTT] did not receive PUBACK for packet ID %d within %v", p.PacketID, deviceKeyValueRetryInterval)

		case ack := <-mc.puback:
			if ack.PacketID == p.PacketID {
				logInfo("[MQTT] received PUBACK for packet ID %d", ack.PacketID)
				return nil
			}
			// TBD: Something is really wrong if packet ID does not match. What to do?
		}

		p.Dup = true
	}

	return errors.New("keyValue dropped")
}

func (dc *DeviceContext) openMQTTConn(cmdQueue chan<- *DeviceCommand, evtQueue <-chan *DeviceEvent, kvQueue <-chan *ActionKeyValue) (*mqttConn, error) {
	mc := &mqttConn{
		dc:   dc,
		subs: make(map[string]mqttSubscription),
	}

	addr := fmt.Sprintf("%s:%d", mqttHost, mqttPort)

	if mqttTLS {
		if conn, err := tls.DialWithDialer(mqttDialer, "tcp", addr, nil); err == nil {
			mc.conn = conn
		} else {
			logError("MQTT TLS dialer failed: %s", err.Error())
			return nil, err
		}
	} else {
		if conn, err := mqttDialer.Dial("tcp", addr); err == nil {
			mc.conn = conn
		} else {
			logError("MQTT dialer failed: %s", err.Error())
			return nil, err
		}
	}

	mc.stream = packet.NewStream(mc.conn, mc.conn)

	if err := mc.connect(); err != nil {
		mc.conn.Close()
		return nil, err
	}

	if err := mc.subscribe(fmt.Sprintf("/devices/%d/command", mc.dc.DeviceID), mc.handleCommandMsg); err != nil {
		mc.conn.Close()
		return nil, err
	}

	mc.command = cmdQueue
	mc.event = evtQueue
	mc.keyValue = kvQueue
	mc.doPing = make(chan time.Duration, 1)
	mc.stopEventProc = make(chan bool)
	mc.err = make(chan error, 10) // make sure this won't block
	mc.outPacket = make(chan packet.Packet, 1)
	mc.puback = make(chan *packet.PubackPacket, 1)
	mc.pingresp = make(chan *packet.PingrespPacket, 1)

	go mc.runPacketReader()
	go mc.runPacketWriter()
	go mc.runUpstreamProcessor()
	go mc.runPinger()

	return mc, nil
}
