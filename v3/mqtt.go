// MQTT Client the Mode MQTT API
// The interface is through the MQTTClient struct, which supports the MQTT
// subset that is required for our devices.
package mode

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	packet "github.com/moderepo/device-sdk-go/v3/mqtt_packet"
)

const (
	mqttConnectTimeout = time.Second * 10
)

var mqttDialer = &net.Dialer{Timeout: mqttConnectTimeout}

// QoS level of message delivery. This is used in sending events to MODE.
type QOSLevel int

const (
	// QoS 0 - message delivery is not guaranteed.
	QOSAtMostOnce QOSLevel = iota

	// QoS 1 - message is delivered at least once, but duplicates may happen.
	QOSAtLeastOnce

	// QoS 2 - message is always delivered exactly once. This is currently not supported.
	QOSExactlyOnce
)

type (
	// Data to send in the channel to the client when we receive data
	// published for our subscription
	MqttSubData struct {
		topic string
		data  []byte
	}

	// Data to send in the channel to the client when we receive the
	// PUBACK on publish, or an error
	MqttQueueResult struct {
		PacketId uint16
		Err      error
	}

	// For users of MqttClient. Three responsibilities (Oops. single
	// responsibility, blah blah.):
	// - data needed to establish a connection
	// - Callback method to create data to publish
	// - channels to communicate data back to a receiver.
	// These methods should be const, since we might call them frequently.
	// I think this could be broken up - maybe a struct for the channels, with
	// another interface (and the struct implement the interface if desired).
	// But this is sufficient for now.
	MqttDelegate interface {
		// Returns authentication information
		AuthInfo() (username string, password string)
		// Returns the 3 channels on which we will receive information:
		// spubRecvCh: Data published from our subscriptions
		// pubAckCh: We will receive MqttPublishID's, which will correspond
		//   to the return value of Publish()
		// pingAckCh: True if our ping received an ACK or false if timeout
		// The delegate is responsible for creating these channels to be of
		// sufficient size. This is the only mechanism for communicating with
		// the user. If necessary, the user can create a queue to ensure
		// that no data is lost.
		ReceiveChannels() (subRecvCh chan<- MqttSubData,
			queueAckCh chan<- MqttQueueResult,
			pingAckCh chan<- bool)

		// How long to wait for requests to finish
		RequestTimeout() time.Duration

		// Retrieve the topics for this delegate
		Subscriptions() []string

		// Hook so we can clean up on closing of connections
		Close()
	}

	MqttClient struct {
		// Provides the public API to MQTT. We handle
		// connect, disconnect, ping (internal)
		// publish, and subscribe. All functions are synchronous.

		isConnected bool
		delegate    MqttDelegate
		conn        *mqttConn
		wgSend      sync.WaitGroup // wait on sending
		wgRecv      sync.WaitGroup // wait on receiving
	}

	// Delegate for the mqqtConn to call back to the MqttClient
	mqttPubReceiver interface {
		handlePubReceive(*packet.PublishPacket)
	}

	// Type alias for something that we use everywhere
	mqttPacketChan chan packet.Packet
	// Internal structure used by the client to communicate with the mqttd
	// server. This is a thin wrapper over the mqtt_packet package.
	mqttConn struct {
		// delegate to handle receiving publishes. Of course, achannel would
		// be an alternate way to do this, but this delegate is currently
		// just doing some verification and getting the bytes out of the
		// packet and putting it into a channel, so adding another channel
		// for that would add unneeded overhead. If circumstances change,
		// change, we can revisit this decision.
		PubHandler mqttPubReceiver

		conn   net.Conn
		stream *packet.Stream
		// Sequential packet ID. Used to match acks our actions (pub, sub)
		// excluding connects and pings. We also don't have packet ID's for
		// receiving pubs from our subscriptions because we didn't initiate them.
		lastPacketID uint16

		// Channels to respond to clients
		packIDToChan map[uint16]mqttPacketChan
		outPktCh     mqttPacketChan
		connRespCh   mqttPacketChan
		pingRespCh   chan<- bool
		queueAckCh   chan<- MqttQueueResult

		mutex sync.Mutex
	}

	MqttClientError error
)

// Creates a client and connects. A client is invalid if not connected, and
// you need to create a new client to reconnect and subscribe
// a pingInterval of zero means no keepalive pings
func NewMqttClient(mqttHost string, mqttPort int, tlsConfig *tls.Config,
	useTLS bool, delegate MqttDelegate) *MqttClient {
	_, queueAckCh, pingAchCh := delegate.ReceiveChannels()
	conn := newMqttConn(tlsConfig, mqttHost, mqttPort, useTLS, queueAckCh,
		pingAchCh)
	if conn == nil {
		return nil
	}

	client := &MqttClient{
		isConnected: false,
		delegate:    delegate,
		conn:        conn,
	}
	conn.PubHandler = client

	// We want to pass our WaitGroup's to the connection reader and writer, so
	// we don't put these in the mqttConn's constructor.
	client.wgSend.Add(1)
	go conn.runPacketWriter(&client.wgSend)
	client.wgRecv.Add(1)
	go conn.runPacketReader(&client.wgRecv)

	// Connect and Subscribe here. These are exposed for now, but we might
	// want to move them to non-public
	return client
}

// Const function, but we don't want to pass client by value
func (client *MqttClient) IsConnected() bool {
	return client.isConnected
}

// exported for now, but pretty sure this needs to be called only from
// NewMqttclient. This blocks until we get a Connection Ack
func (client *MqttClient) Connect() error {
	user, pwd := client.delegate.AuthInfo()
	p := packet.NewConnectPacket()
	p.Version = packet.Version311
	p.Username = user
	p.Password = pwd
	p.CleanSession = true

	respChan, err := client.conn.SendPacket(p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	var pkt packet.Packet
	client.wgRecv.Add(1)
	if pkt, err = client.receivePacket(respChan); err != nil {
		logError("[MQTT] failed to receive packet %s", err)
		return err
	}

	if pkt.Type() != packet.CONNACK {
		logError("[MQTT] received unexpected packet %s", pkt.Type())
		return errors.New("unexpected response")
	}

	ack := pkt.(*packet.ConnackPacket)
	if ack.ReturnCode != packet.ConnectionAccepted {
		return fmt.Errorf("connection failed: %s", ack.ReturnCode.Error())
	}

	// If we made it here, we consider ourselves connected
	client.isConnected = true

	return nil
}

// This blocks until we receive an ACK (really, an EOF) from the server and
// all our connections are done
func (client *MqttClient) Disconnect() error {
	defer func() {
		client.delegate.Close()
		client.wgSend.Wait() // wait for packet sender to finish

		// Disconnects don't send a packet. Just make sure we wait for the EOF
		defer client.wgRecv.Wait()
	}()

	// Creates a new context, using the client's timeout
	p := packet.NewDisconnectPacket()
	_, err := client.conn.SendPacket(p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}
	client.conn.Close()

	return nil
}

// Add the subscriptions from the delegate
func (client *MqttClient) Subscribe() error {
	p := packet.NewSubscribePacket()
	p.Subscriptions = make([]packet.Subscription, 0, 10)

	subs := client.delegate.Subscriptions()
	for _, sub := range subs {
		// We have no protection to keep you from subscribing to the same
		// topic multiple times. Maybe we should? Maybe the server would send
		// us 2 messages then for each topic?
		logInfo("[MQTT] subscribing to topic %s", sub)
		p.Subscriptions = append(p.Subscriptions, packet.Subscription{
			Topic: sub,
			// MODE only supports QoS0 for subscriptions
			QOS: packet.QOSAtMostOnce,
		})
	}

	respChan, err := client.conn.SendPacket(p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	var pkt packet.Packet
	client.wgRecv.Add(1)
	if pkt, err = client.receivePacket(respChan); err != nil {
		return err
	}

	if pkt.Type() != packet.SUBACK {
		logError("[MQTT] received unexpected packet %s", pkt.Type())
		return errors.New("unexpected response")
	}

	ack := pkt.(*packet.SubackPacket)
	if len(ack.ReturnCodes) != len(subs) {
		logError("[MQTT] received SUBACK packet with incorrect number of return codes: expect %d; got %d", len(subs), len(ack.ReturnCodes))
		return errors.New("invalid packet")
	}

	for i, code := range ack.ReturnCodes {
		sub := subs[i]

		if code == packet.QOSFailure {
			logError("[MQTT] subscription to topic %s rejected", sub)
			return errors.New("subscription rejected")
		}

		logInfo("[MQTT] subscription to topic %s succeeded with QOS %v",
			sub, code)
	}

	return nil
}

func (client *MqttClient) Ping() error {

	p := packet.NewPingreqPacket()

	_, err := client.conn.QueuePacket(p, client.delegate.RequestTimeout())
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	return nil
}

func (client *MqttClient) Publish(qos QOSLevel, topic string,
	data []byte) (uint16, error) {

	var pktQos byte
	switch qos {
	case QOSAtMostOnce:
		pktQos = packet.QOSAtMostOnce
	case QOSAtLeastOnce:
		pktQos = packet.QOSAtLeastOnce
	default:
		return 0, errors.New("unsupported qos level")
	}

	p := packet.NewPublishPacket()

	p.Message = packet.Message{
		Topic:   topic,
		QOS:     pktQos,
		Payload: data,
	}
	logInfo("Publishing on topic [%s]", p.Message.Topic)

	return client.conn.QueuePacket(p, client.delegate.RequestTimeout())
}

func (client *MqttClient) receivePacket(respChan mqttPacketChan) (packet.Packet,
	error) {
	defer client.wgRecv.Done()

	ctx, cancel := context.WithTimeout(context.Background(),
		client.delegate.RequestTimeout())
	defer cancel()

	// Block on the return channel or timeout
	select {
	case pkt := <-respChan:
		// If this happens, we were blocked on a channel waiting for a packet
		// and the server closed the connection
		if pkt == nil {
			return pkt, errors.New("connection closed")
		} else {
			return pkt, nil
		}
	case <-ctx.Done():
		err := ctx.Err()
		logError("[MQTT] timeout waiting for reply packet %s", err)
		return nil, err
	}
}

// Implementation of the delegate for mqttConn to handle publish from the server
// on topics that we've subscribed to. We only unpack it and send it to the
// caller
func (client *MqttClient) handlePubReceive(p *packet.PublishPacket) {
	logInfo("[MQTT] received message for topic %s", p.Message.Topic)

	pubData := MqttSubData{p.Message.Topic, p.Message.Payload}

	subRecvCh, _, _ := client.delegate.ReceiveChannels()
	select {
	case subRecvCh <- pubData:
	default:
		logError("Caller could not receive publish data. Channel full?")
	}
}

func newMqttConn(tlsConfig *tls.Config, mqttHost string,
	mqttPort int, useTLS bool, queueAckCh chan<- MqttQueueResult,
	pingAckCh chan<- bool) *mqttConn {

	addr := fmt.Sprintf("%s:%d", mqttHost, mqttPort)
	var conn net.Conn
	var err error
	if useTLS {
		if conn, err = tls.DialWithDialer(mqttDialer, "tcp", addr,
			tlsConfig); err == nil {
		} else {
			logError("MQTT TLS dialer failed: %s", err.Error())
			return nil
		}
	} else {
		if conn, err = mqttDialer.Dial("tcp", addr); err == nil {
		} else {
			logError("MQTT dialer failed: %s", err.Error())
			return nil
		}
	}

	return &mqttConn{
		conn:   conn,
		stream: packet.NewStream(conn, conn),
		// This map will grow to the size of the number of requests that are
		// unanswered. These block, so a size of 1 is sufficient.
		packIDToChan: make(map[uint16]mqttPacketChan, 1),
		// Channel to the mqtt server. This can possibly get backed up on a slow
		// network. The size of 8 is arbitrary. If it is full, we will block
		// until a timeout (specified by the delegate) and return an error.
		outPktCh: make(mqttPacketChan, 8),
		// Connection requests are blocked, so 1 is sufficient
		connRespCh: make(mqttPacketChan, 1),
		// These are created by the delegate, so it's up to them to create
		// channels of sufficient size or we will fail.
		pingRespCh: pingAckCh,
		queueAckCh: queueAckCh,
	}
}

func (conn *mqttConn) Close() {
	conn.closeAllChannels()
}

// We only queue pings (PINGREQ) and publishes (PUBLISH). Theoretically, we
// could queue subscribes (SUBSCRIBE) since they have packet ID's like
// publishes. But, to be able to handle queueing and sending for the same type
// of packet would make the code overly complicated.
func (conn *mqttConn) QueuePacket(p packet.Packet,
	timeout time.Duration) (uint16, error) {

	var packetID uint16
	switch p.Type() {
	case packet.PINGREQ:
		// We send these as is, without adding a packet ID
	case packet.PUBLISH:
		pubPkt := p.(*packet.PublishPacket)
		packetID = conn.getPacketID()
		pubPkt.PacketID = uint16(packetID)
	default:
		logError("[MQTT] Unhandled packet type: %s", p.Type())
		return 0, fmt.Errorf("Unhandled packet type for queueing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case conn.outPktCh <- p:
		// Successfully sent, so nothing to do.
	case <-ctx.Done():
		logError("Exceeded timeout sending %s for id %d", p.Type(), packetID)
		return 0, fmt.Errorf("Send Queue full %s for id %d", p.Type(), packetID)
	}

	return packetID, nil
}

// Called by the client to send packets to the server.
func (conn *mqttConn) SendPacket(p packet.Packet) (mqttPacketChan, error) {

	ch := conn.getResponseChan(p)

	err := conn.writePacket(p)

	return ch, err
}

func (conn *mqttConn) runPacketWriter(wg *sync.WaitGroup) {
	defer func() {
		logInfo("[MQTT] packet writer is exiting")
		wg.Done()
	}()

	// Run until closed
	for p := range conn.outPktCh {
		err := conn.writePacket(p)
		if err != nil {
			logError("Unable to write out packet %s", p.Type())
			switch p.Type() {
			case packet.PINGREQ:
				conn.pingRespCh <- false
			case packet.SUBSCRIBE:
				subPkt := p.(*packet.SubscribePacket)
				conn.queueAckCh <- MqttQueueResult{subPkt.PacketID, err}
			case packet.PUBLISH:
				pubPkt := p.(*packet.PublishPacket)
				conn.queueAckCh <- MqttQueueResult{pubPkt.PacketID, err}
			default:
				// We don't have a packet ID (or did not expect to receive this
				// packet type and fail in sending it
				err := fmt.Errorf("Failed to queue packet %s", p.Type())
				conn.queueAckCh <- MqttQueueResult{0, err}
			}
		}
	}
}

func (conn *mqttConn) runPacketReader(wg *sync.WaitGroup) {
	defer func() {
		logInfo("[MQTT] packet reader is exiting")
		wg.Done()
		// Close the rest of our channels?
	}()

	for {
		p, err := conn.stream.Read()
		if err != nil {
			if err == io.EOF {
				// Server disconnected. This happens for 2 reasons:
				// 1. we disconnect
				// 2. we don't ping, so the server assumes we're done
				// XXX we should wait for the EOF before we exit.
				// In any case, we have to see if upstream this wasn't
				// expected
				// it wasn't expected upstream
				logInfo("[MQTT] server disconnected: %s", err.Error())
			} else {
				logError("[MQTT] failed to read packet: %s", err.Error())
				//conn.reportError(err)
			}
			break
		}
		if p.Type() == packet.PUBLISH {
			// Incoming, received from our subscription. (all outher packets
			// are outbound.
			pubPkt := p.(*packet.PublishPacket)
			conn.PubHandler.handlePubReceive(pubPkt)
		} else {
			respChan := conn.getResponseChan(p)
			if respChan != nil {
				respChan <- p
				if p.Type() == packet.SUBACK {
					subAck := p.(*packet.SubackPacket)
					delete(conn.packIDToChan, subAck.PacketID)
					close(respChan)
				}
			} else {
				// Figure out what the packet was. If it's nil, it should
				// be a response to a queued delivery
				if p.Type() == packet.PINGRESP {
					conn.pingRespCh <- true
				} else if p.Type() == packet.PUBACK {
					pubAck := p.(*packet.PubackPacket)
					conn.queueAckCh <- MqttQueueResult{pubAck.PacketID, nil}
				} else if p.Type() == packet.SUBACK {
					subAck := p.(*packet.SubackPacket)
					conn.queueAckCh <- MqttQueueResult{subAck.PacketID, nil}
				}
			}
		}
	}
}

func (conn *mqttConn) writePacket(p packet.Packet) error {
	// Since we have both queued writes, and blocking, we lock at the
	// lowest level
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if err := conn.stream.Write(p); err != nil {
		logError("[MQTT] failed to send %s packet: %s", p.Type(), err.Error())
		return err
	}

	if err := conn.stream.Flush(); err != nil {
		logError("[MQTT] failed to flush %s packet: %s", p.Type(), err.Error())
		return err
	}

	return nil
}

func (conn *mqttConn) closeAllChannels() {

	for _, v := range conn.packIDToChan {
		close(v)
	}

	close(conn.outPktCh)
	close(conn.connRespCh)
	close(conn.pingRespCh)
}

func (conn *mqttConn) getPacketID() uint16 {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	conn.lastPacketID += 1
	if conn.lastPacketID == 0 {
		conn.lastPacketID = 1
	}
	return conn.lastPacketID
}

// Returns the appropriate response channel for the channel type. For pubacks,
// We will either create it or return the one that we stored previous
func (conn *mqttConn) getResponseChan(p packet.Packet) mqttPacketChan {
	//logInfo("[MQTT] Fetching response channel for %s\n", p.Type())
	switch p.Type() {
	case packet.CONNECT:
		return conn.connRespCh
	case packet.DISCONNECT:
		// Disconnect is a special case. Server will close, so no response, but
		// it's a packet type we expect, so don't let it fall to default
		return nil
	case packet.CONNACK:
		return conn.connRespCh
	case packet.SUBSCRIBE:
		// Subscribes are blocking, but have packet ID's, and, theoretically,
		// multiples can be sent, so we enable this at this level.
		subPkt := p.(*packet.SubscribePacket)
		subPkt.PacketID = conn.getPacketID()
		returnCh := make(mqttPacketChan)
		conn.packIDToChan[subPkt.PacketID] = returnCh
		return returnCh
	case packet.SUBACK:
		// It's possible the SUBACK doesn't have a response channel if it
		// was queued. In that case, we'll return a nil and handle it like
		// PINGREQ or PUBACK
		subAck := p.(*packet.SubackPacket)
		return conn.packIDToChan[subAck.PacketID]
	case packet.PUBLISH, packet.PINGREQ, packet.PUBACK, packet.PINGRESP:
		// Since we have to respond differently to these, but we don't
		// want to show an error
		return nil
	default:
		//
		logError("[MQTT] Unhandled packet type: %s", p.Type())
		return nil
	}
}

var (
	infoLogger  = log.New(os.Stdout, "[MODE - INFO] ", log.LstdFlags)
	errorLogger = log.New(os.Stderr, "[MODE - ERROR] ", log.LstdFlags)
)

func logInfo(format string, values ...interface{}) {
	infoLogger.Printf(format+"\n", values...)
}

func logError(format string, values ...interface{}) {
	errorLogger.Printf(format+"\n", values...)
}
