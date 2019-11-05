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
	"net"
	"sync"
	"time"

	packet "github.com/moderepo/device-sdk-go/v2/mqtt_packet"
)

const (
	mqttConnectTimeout = time.Second * 10
)

var mqttDialer = &net.Dialer{Timeout: mqttConnectTimeout}

type (
	MqttPubHandler func(*packet.PublishPacket) error

	MqttSubscription struct {
		topic      string
		msgHandler MqttPubHandler
	}

	// For implementation dependent MQTT.
	MqttDelegate interface {
		// Returns authentication information
		AuthInfo() (username string, password string)
		// Builds the publish packet based on data in the interface.
		BuildPublishPacket(data interface{}) (packet.Packet, error)
		Subscriptions() []MqttSubscription
	}

	MqttClient struct {
		// Provides the public API to MQTT. We handle
		// connect, disconnect, ping (internal)
		// publish, and subscribe. All functions are synchronous.

		isConnected  bool
		delegate     MqttDelegate  // mqtt conn's need user/pass
		deadline     time.Duration // our timeout duration
		pingInterval time.Duration // our timeout duration
		conn         *mqttConn
		subs         map[string]MqttSubscription // map of subs to handlers
		pingCtrl     chan bool                   // Channel to stop pings
		wgSend       sync.WaitGroup              // wait on sending
		wgRecv       sync.WaitGroup              // wait on receiving
	}

	// Delegate for the connection to call back to the client
	mqttPubReceiver interface {
		handlePub(*packet.PublishPacket)
	}

	// Type alias for something that we use everywhere
	responseChan chan packet.Packet

	// Internal structure used by the client to communicate with the mqttd
	// server
	mqttConn struct {
		PubHandler mqttPubReceiver // delegate to handle publish

		conn   net.Conn
		stream *packet.Stream
		// Sequential packet ID. Used to match acks our actions (pub, sub)
		// excluding connects and pings. We also don't have packet ID's for
		// receiving pubs from our subscriptions because we didn't initiate them.
		lastPacketID uint16

		// Channels to respond to clients
		packIDToChan map[uint16]responseChan
		connRespCh   responseChan
		pingRespCh   responseChan

		pubHandleCh responseChan
	}

	MqttClientError error
)

// Creates a client and connects. A client is invalid if not connected, and
// you need to create a new client to reconnect and subscribe
// a pingInterval of zero means no keepalive pings
func NewMqttClient(mqttHost string, mqttPort int, tlsConfig *tls.Config,
	useTLS bool, requestTimeout time.Duration, pingInterval time.Duration,
	delegate MqttDelegate) *MqttClient {
	conn := newMqttConn(tlsConfig, mqttHost, mqttPort, useTLS)

	if conn == nil {
		return nil
	}

	client := &MqttClient{
		isConnected:  false,
		delegate:     delegate,
		deadline:     requestTimeout,
		pingInterval: pingInterval,
		conn:         conn,
		subs:         make(map[string]MqttSubscription),
		pingCtrl:     make(chan bool),
	}
	conn.PubHandler = client

	go conn.RunPacketReader(&client.wgRecv)

	return client
}

func (client MqttClient) IsConnected() bool {
	return client.isConnected
}

// exported for now, but pretty sure this needs to be called only from
// NewMqttclient
func (client *MqttClient) Connect() error {
	// Creates a new context, using the client's timeout
	ctx := client.createContext()

	user, pwd := client.delegate.AuthInfo()
	p := packet.NewConnectPacket()
	p.Version = packet.Version311
	p.Username = user
	p.Password = pwd
	p.CleanSession = true

	respChan, err := client.conn.SendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	var pkt packet.Packet
	if pkt, err = client.receivePacket(ctx, respChan); err != nil {
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
	client.setIsConnected(true)

	return nil
}

func (client *MqttClient) Disconnect() error {

	// signal the ping loop to exit
	close(client.pingCtrl)

	logInfo("MQTT: Waiting for packet reader to finish")
	client.wgSend.Wait() // wait for packet sender to finish

	// Creates a new context, using the client's timeout
	ctx := client.createContext()
	p := packet.NewDisconnectPacket()
	_, err := client.conn.SendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	// Disconnects don't send a packet, so just make sure we wait for the EOF
	defer client.wgRecv.Wait() // wait for packet reader to finish

	return nil
}

// Add the subscriptions from the delegate
func (client *MqttClient) AddSubscriptions() error {
	ctx := client.createContext()

	p := packet.NewSubscribePacket()
	p.Subscriptions = make([]packet.Subscription, 0, 10)

	subs := client.delegate.Subscriptions()
	for _, s := range subs {
		logInfo("[MQTT] subscribing to topic %s", s.topic)
		if _, exists := client.subs[s.topic]; exists {
			// We could abort, but then we need to do extra work. For now,
			// just skip it.
			logError("Subscription for exists: %s. Skipping", s.topic)
		} else {
			p.Subscriptions = append(p.Subscriptions, packet.Subscription{
				Topic: s.topic,
				QOS:   packet.QOSAtMostOnce, // MODE only supports QoS0 for subscriptions
			})
		}
	}

	respChan, err := client.conn.SendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	var pkt packet.Packet
	if pkt, err = client.receivePacket(ctx, respChan); err != nil {
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
		s := subs[i]

		if code == packet.QOSFailure {
			logError("[MQTT] subscription to topic %s rejected", s.topic)
			return errors.New("subscription rejected")
		}

		logInfo("[MQTT] subscription to topic %s succeeded with QOS %v", s.topic, code)
		client.subs[s.topic] = s
	}
	fmt.Println("Successfully subscribed")
	return nil
}

func (client *MqttClient) Ping() error {
	// Creates a new context, using the client's timeout
	ctx := client.createContext()

	p := packet.NewPingreqPacket()

	respChan, err := client.conn.SendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	var pkt packet.Packet
	logInfo("[MQTT] waiting for PINGRESP packet")
	if pkt, err = client.receivePacket(ctx, respChan); err != nil {
		return err
	}

	if pkt.Type() != packet.PINGRESP {
		logError("[MQTT] received unexpected packet %s", pkt.Type())
		return errors.New("unexpected response")
	}
	return nil
}

func (client *MqttClient) Publish(data interface{}) error {
	ctx := client.createContext()

	var p packet.Packet
	var err error
	if p, err = client.delegate.BuildPublishPacket(data); err != nil {
		logError("[MQTT] delegate failed: %s", err.Error())
		return err
	}

	respChan, err := client.conn.SendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	var pkt packet.Packet
	logInfo("[MQTT] waiting for PUBLISH response")
	if pkt, err = client.receivePacket(ctx, respChan); err != nil {
		return err
	}

	if pkt.Type() != packet.PUBACK {
		logError("[MQTT] received unexpected packet %s", pkt.Type())
		return errors.New("unexpected response")
	}
	logInfo("[MQTT] publish completed")
	return nil
}

func (client *MqttClient) setIsConnected(isConnected bool) {
	client.isConnected = isConnected
	if isConnected && client.pingInterval > 0 {
		go client.runPinger(client.pingInterval)
	}
}

func (client *MqttClient) runPinger(pingInterval time.Duration) {
	logInfo("[MQTT] pinger is running")
	client.wgSend.Add(1)
	ticker := time.NewTicker(pingInterval)
	// XXX - need another channel to tell us to stop
	defer func() {
		ticker.Stop()
		client.wgSend.Done()
		logInfo("[MQTT] pinger is exiting")
	}()

	for {
		if client.Ping() != nil {
			logError("[MQTT] error in ping. exiting pinger")
			break
		}
		select {
		case <-ticker.C: // Wait for the next ticker
			continue
		case <-client.pingCtrl: // Stop pinging
			return
		}
	}
}

func (client *MqttClient) createContext() context.Context {
	if client.deadline > 0 {
		d := time.Now().Add(client.deadline)
		ctx, _ := context.WithDeadline(context.Background(), d)
		return ctx
	} else {
		return context.Background()
	}
}

func (client *MqttClient) receivePacket(ctx context.Context,
	respChan responseChan) (packet.Packet, error) {

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

func (client *MqttClient) handlePub(p *packet.PublishPacket) {
	sub, exists := client.subs[p.Message.Topic]
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

func newMqttConn(tlsConfig *tls.Config, mqttHost string,
	mqttPort int, useTLS bool) *mqttConn {

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
		// unanswered
		packIDToChan: make(map[uint16]responseChan),
		connRespCh:   make(responseChan),
		pingRespCh:   make(responseChan),
		pubHandleCh:  make(responseChan),
	}
}

// Called by the client to send packets to the server.
func (conn *mqttConn) SendPacket(ctx context.Context,
	p packet.Packet) (responseChan, error) {

	ch := conn.getResponseChan(p)
	//logInfo("[MQTT]: sending packet: ", p)
	if err := conn.stream.Write(p); err != nil {
		logError("[MQTT] failed to send %s packet: %s", p.Type(), err.Error())
		return nil, err
	}

	if err := conn.stream.Flush(); err != nil {
		logError("[MQTT] failed to flush %s packet: %s", p.Type(), err.Error())
		return nil, err
	}

	return ch, nil
}

// Called by the client to start listening for packets from the server
func (conn *mqttConn) RunPacketReader(wg *sync.WaitGroup) {
	wg.Add(1)

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
			conn.closeAllChannels()
			break
		}
		if p.Type() == packet.PUBLISH {
			pubPkt := p.(*packet.PublishPacket)
			conn.PubHandler.handlePub(pubPkt)
		} else {
			respChan := conn.getResponseChan(p)
			respChan <- p
			// After we write the packet back, we might need to do some cleanup
			// for puback channels, we close it and remove it from the map
			if p.Type() == packet.PUBACK {
				pubAck := p.(*packet.PubackPacket)
				delete(conn.packIDToChan, pubAck.PacketID)
				close(respChan)
			}
		}
	}
}

func (conn *mqttConn) closeAllChannels() {

	for _, v := range conn.packIDToChan {
		close(v)
	}

	close(conn.connRespCh)
	close(conn.pingRespCh)
	close(conn.pubHandleCh)
}

func (conn *mqttConn) getPacketID() uint16 {
	conn.lastPacketID += 1
	if conn.lastPacketID == 0 {
		conn.lastPacketID = 1
	}
	return conn.lastPacketID
}

// Returns the appropriate response channel for the channel type. For pubacks,
// We will either create it or return the one that we stored previous
func (conn *mqttConn) getResponseChan(p packet.Packet) responseChan {
	logInfo("[MQTT] Fetching response channel for %s\n", p.Type())
	switch p.Type() {
	case packet.CONNECT:
		return conn.connRespCh
	case packet.DISCONNECT:
		// Disconnect is a special case. Server will close, so no response, but
		// it's a packet type we expect, so don't let it fall to default
		return nil
	case packet.CONNACK:
		return conn.connRespCh
	case packet.PINGREQ:
		return conn.pingRespCh
	case packet.PINGRESP:
		return conn.pingRespCh

	// These two pairs are exactly the same, just the different name of the
	// packet. I think this is where generics would come in handy
	case packet.SUBSCRIBE:
		subPkt := p.(*packet.SubscribePacket)
		if subPkt.PacketID == 0 {
			subPkt.PacketID = conn.getPacketID()
		}
		returnCh := make(responseChan)
		conn.packIDToChan[subPkt.PacketID] = returnCh
		return returnCh
	case packet.PUBLISH:
		pubPkt := p.(*packet.PublishPacket)
		if pubPkt.PacketID == 0 {
			pubPkt.PacketID = conn.getPacketID()
		}
		returnCh := make(responseChan)
		conn.packIDToChan[pubPkt.PacketID] = returnCh
		return returnCh
	case packet.SUBACK:
		subAck := p.(*packet.SubackPacket)
		return conn.packIDToChan[subAck.PacketID]
	case packet.PUBACK:
		pubAck := p.(*packet.PubackPacket)
		return conn.packIDToChan[pubAck.PacketID]
	default:
		logError("[MQTT] Unhandled packet type: %s", p.Type())
		return nil
	}
}
