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

	// QoS 2 - message is always delivered exactly once. This is currently not
	// supported.
	QOSExactlyOnce
)

type packetSendType int

const (
	// Packets are written out to the stream immediately (or after the
	// the previous packet has finished sending)
	directPacketSendType packetSendType = iota
	// Packets are queued and written when processed
	queuedPacketSendType
	unhandledPacketSendType
)

type NetworkStatus int

const (
	ConnectedNetworkStatus NetworkStatus = iota
	DisconnectedNetworkStatus
	TimingOutNetworkStatus
	DefaultNetworkStatus
)

type (
	// Data to send in the channel to the client when we receive data
	// published for our subscription
	MqttSubData struct {
		topic string
		data  []byte
	}

	// Response data for an MQTT Request. Not all of these functions will be
	// valid. For example, only PUBACK's will have PacketIds, and PUBLISH'es
	// will send subscription data. In some cases, there will be multiple
	MqttResponse struct {
		PacketID uint16
		Err      error
		Errs     []error
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
		// Returns the tls usage and configuration. If useTLS is false, a nil
		// tlsConfig should be returned.
		TLSUsageAndConfiguration() (useTLS bool, tlsConfig *tls.Config)
		// Returns authentication information
		AuthInfo() (username string, password string)
		// Returns the 3 channels on which we will receive information:
		// subRecvCh: Data published from our subscriptions.
		// queueAckCh: API requests that are queued will receive MqttPublishID's
		// which will be ACK'ed. The MqttQueueResult will have the
		// MqttPublishID and the result
		// pingAckCh: True if our ping received an ACK or false if timeout
		// The delegate is responsible for creating these channels to be of
		// sufficient size. This is the only mechanism for communicating with
		// the user. If necessary, the user can create a queue to ensure
		// that no data is lost.
		GetChannels() (subRecvCh chan<- MqttSubData,
			queueAckCh chan MqttResponse,
			pingAckCh chan MqttResponse)

		// Buffer size of the outgoing queue to the server. This cannot be
		// changed after a client is created
		OutgoingQueueSize() uint16

		// Retrieve the topics for this delegate
		GetSubscriptions() []string

		// Hook so we can clean up on closing of connections
		OnClose()
	}

	MqttClient struct {
		// Provides the public API to MQTT. We handle
		// connect, disconnect, ping, publish, and subscribe.
		// Connect, disconnect, and subscribe will block and wait for the
		// response. Ping and publish will return after the packet has been
		// sent and the response will be sent on a channel that is provided
		// by the delegate. For ping, since MQTT does not provide a mechanism
		// to distinguish between different ping requests, we do not provide
		// an API to distinguish them either. For publish, the function returns
		// a packet ID. This packet ID will be returned to the caller in the
		// channel.

		delegate MqttDelegate
		conn     *mqttConn
		wgSend   sync.WaitGroup // wait on sending
		wgRecv   sync.WaitGroup // wait on receiving
	}

	// Delegate for the mqqtConn to call back to the MqttClient
	mqttPubReceiver interface {
		handlePubReceive(*packet.PublishPacket)
	}

	packetSendData struct {
		pkt      packet.Packet
		resultCh chan<- MqttResponse
	}

	// Type alias for something that we use everywhere
	mqttSendPacketChan chan packetSendData

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
		// Sequential packet ID. Used to match to acks our actions (pub)
		// excluding connects and pings. We also don't have packet ID's for
		// receiving pubs from our subscriptions because we didn't initiate them.
		lastPacketID uint16

		status NetworkStatus

		// Updated on every send and receive so we know if we can avoid
		// sending pings
		lastActivity time.Time

		// Channel to write to the server stream
		directSendPktCh chan packetSendData
		queuedSendPktCh chan packetSendData
		// Channels to respond to clients.
		connRespCh chan MqttResponse
		subRespCh  chan MqttResponse
		pingRespCh chan MqttResponse
		queueAckCh chan MqttResponse

		mutex sync.Mutex
	}
)

// Creates a client and connects. A client is invalid if not connected, and
// you need to create a new client to reconnect and subscribe
// a pingInterval of zero means no keepalive pings
func NewMqttClient(mqttHost string, mqttPort int,
	delegate MqttDelegate) *MqttClient {
	_, queueAckCh, pingAchCh := delegate.GetChannels()
	useTLS, tlsConfig := delegate.TLSUsageAndConfiguration()
	outgoingQueueSize := delegate.OutgoingQueueSize()
	// XXX - we pass quit a few arguments to the connection. Maybe we should
	// pass in the delegate instead. This allows the delegate to provide the
	// connection to give data to dynamically
	conn := newMqttConn(tlsConfig, mqttHost, mqttPort, useTLS, queueAckCh,
		pingAchCh, outgoingQueueSize)
	if conn == nil {
		return nil
	}

	client := &MqttClient{
		delegate: delegate,
		conn:     conn,
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
	return client.conn.status == ConnectedNetworkStatus
}

// GetLastActivity will return the time since the last send or
// receive.
func (client *MqttClient) GetLastActivity() time.Time {
	return client.conn.lastActivity
}

// exported for now, but pretty sure this needs to be called only from
// NewMqttclient. This blocks until we get a Connection Ack
func (client *MqttClient) Connect(ctx context.Context) error {
	user, pwd := client.delegate.AuthInfo()
	p := packet.NewConnectPacket()
	p.Version = packet.Version311
	p.Username = user
	p.Password = pwd
	p.CleanSession = true

	respChan, err := client.conn.sendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	client.wgRecv.Add(1)
	resp := client.receivePacket(ctx, respChan)
	if resp.Err != nil {
		client.conn.status = DisconnectedNetworkStatus
		return resp.Err
	}
	// If we made it here, we consider ourselves connected
	client.conn.status = ConnectedNetworkStatus

	return nil
}

// This blocks until we receive an ACK (really, an EOF) from the server and
// all our connections are done
func (client *MqttClient) Disconnect(ctx context.Context) error {
	defer func() {
		client.conn.close()
		client.delegate.OnClose()
		client.wgSend.Wait() // wait for packet sender to finish

		// Disconnects don't send a packet. Just make sure we wait for the EOF
		// (Alternate implementation: Just tell the receiver to finish by
		// closing a done channel (or the read stream.
		client.wgRecv.Wait()
	}()

	p := packet.NewDisconnectPacket()
	_, err := client.conn.sendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	return nil
}

// Add the subscriptions from the delegate
func (client *MqttClient) Subscribe(ctx context.Context) []error {
	p := packet.NewSubscribePacket()
	p.Subscriptions = make([]packet.Subscription, 0, 10)
	p.PacketID = client.conn.getPacketID()

	subs := client.delegate.GetSubscriptions()
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

	respChan, err := client.conn.sendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return []error{err}
	}

	client.wgRecv.Add(1)
	resp := client.receivePacket(ctx, respChan)
	if resp.Errs != nil {
		return resp.Errs
	}

	return nil
}

func (client *MqttClient) Ping(ctx context.Context) error {

	p := packet.NewPingreqPacket()
	_, err := client.conn.queuePacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	return nil
}

func (client *MqttClient) Publish(ctx context.Context, qos QOSLevel,
	topic string, data []byte) (uint16, error) {

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

	return client.conn.queuePacket(ctx, p)
}

// Helper function called by the synchronous API to handle processing
// of the responses. For the asynchronous API, the caller might do something
// similar, but also handling packet ID's.
func (client *MqttClient) receivePacket(ctx context.Context,
	respChan chan MqttResponse) MqttResponse {
	defer func() {
		client.wgRecv.Done()
		logInfo("receivePacket done")
	}()

	// Block on the return channel or timeout
	select {
	case response := <-respChan:
		return response
	case <-ctx.Done():
		err := ctx.Err()
		//debug.PrintStack()
		logError("[MQTT] timeout waiting for reply packet %s", err)
		return MqttResponse{Err: err}
	}
}

// Implementation of the delegate for mqttConn to handle publish from the server
// on topics that we've subscribed to. We only unpack it and send it to the
// caller
func (client *MqttClient) handlePubReceive(p *packet.PublishPacket) {
	logInfo("[MQTT] received message for topic %s", p.Message.Topic)

	pubData := MqttSubData{p.Message.Topic, p.Message.Payload}

	subRecvCh, _, _ := client.delegate.GetChannels()
	select {
	case subRecvCh <- pubData:
	default:
		logError("Caller could not receive publish data. SubRecCh full?")
	}
}

func newMqttConn(tlsConfig *tls.Config, mqttHost string,
	mqttPort int, useTLS bool, queueAckCh chan MqttResponse,
	pingAckCh chan MqttResponse, outgoingQueueSize uint16) *mqttConn {

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
		status: DefaultNetworkStatus,

		// The two channels that we write packets to send. The packetWriter
		// will listen on these and write out to the stream.
		// blocking, non-buffered
		directSendPktCh: make(chan packetSendData),
		// blocking, queue size specified by the delegate
		queuedSendPktCh: make(chan packetSendData, outgoingQueueSize),

		// Connection requests are blocking, so no buffer
		connRespCh: make(chan MqttResponse),
		subRespCh:  make(chan MqttResponse),

		// These are created by the delegate, so it's up to them to create
		// channels of sufficient size or we will fail.
		pingRespCh: pingAckCh,
		queueAckCh: queueAckCh,
	}
}

func (conn *mqttConn) close() {
	close(conn.directSendPktCh)
	close(conn.queuedSendPktCh)
	close(conn.connRespCh)
	close(conn.pingRespCh)
}

// We only queue pings (PINGREQ) and publishes (PUBLISH). Theoretically, we
// could queue subscribes (SUBSCRIBE) since they have packet ID's like
// publishes. But, for simplicity, those are synchronous, since, in practice,
// those are either on startup, or, at least, on rare occasions.
func (conn *mqttConn) queuePacket(ctx context.Context,
	p packet.Packet) (uint16, error) {
	if conn.status == DisconnectedNetworkStatus ||
		conn.status == TimingOutNetworkStatus {
		return 0, fmt.Errorf("Connection unstable. Unable to send")
	}

	packetID := uint16(0)
	if p.Type() == packet.PUBLISH {
		packetID = conn.getPacketID()
		pubPkt := p.(*packet.PublishPacket)
		pubPkt.PacketID = uint16(packetID)
	}

	pktSendData := packetSendData{pkt: p,
		resultCh: conn.getResponseChannel(p.Type()),
	}

	select {
	// Put writing to the channel in a select because the buffer might be full
	case conn.queuedSendPktCh <- pktSendData:
		// Successfully sent and we don't know when we'll get a response back,
		// so just don't do anything.
	case <-ctx.Done():
		logError("Exceeded timeout sending %s for id %d", p.Type(), packetID)
		return 0, fmt.Errorf("Send Queue full %s for id %d", p.Type(), packetID)
	}

	return packetID, nil
}

// Called by the client to send packets to the server
func (conn *mqttConn) sendPacket(ctx context.Context,
	p packet.Packet) (chan MqttResponse, error) {
	if conn.status == DisconnectedNetworkStatus ||
		conn.status == TimingOutNetworkStatus {
		return nil, fmt.Errorf("Connection unstable. Unable to send")
	}

	resultCh := make(chan MqttResponse)
	defer close(resultCh)

	select {
	case conn.directSendPktCh <- packetSendData{
		pkt:      p,
		resultCh: resultCh,
	}:
	case <-ctx.Done():
		logError("Exceeded timeout sending %s", p.Type())
		return nil, errors.New("Timeout Error")
	}

	// Wait for the result, and then give that result back to the caller, who
	// will handle the error
	result := <-resultCh
	return conn.getResponseChannel(p.Type()), result.Err
}

func (conn *mqttConn) runPacketWriter(wg *sync.WaitGroup) {
	defer func() {
		logInfo("[MQTT] packet writer is exiting")
		wg.Done()
	}()

	shouldLoop := true
	for shouldLoop {
		// We read two channels for writing out packets. The directSend
		// channel received synchronous, which has an unbuffered queue.
		// The queuedSend can back up because the client already has an ID to
		// check for the response.
		// NOTE: Writes are sent on a bufio.Writer, so writes are almost
		// guaranteed to succeed, even if they never reach the server. See the
		// note in writePacket for more information.
		select {
		case pktSendData := <-conn.directSendPktCh:
			resultCh := pktSendData.resultCh
			// Verify that we have the write packet type for direct sends
			if pktSendData.pkt == nil {
				shouldLoop = false
				break
			}
			if conn.verifyPacketType(pktSendData.pkt.Type()) != directPacketSendType {
				resultCh <- MqttResponse{
					Err: fmt.Errorf("Packet type must be queued"),
				}
			} else {
				resultCh <- MqttResponse{Err: conn.writePacket(pktSendData.pkt)}
			}
		case pktSendData := <-conn.queuedSendPktCh:
			if pktSendData.pkt == nil {
				shouldLoop = false
				break
			}
			pktID := uint16(0)
			if pktSendData.pkt.Type() == packet.PUBLISH {
				pubPkt := pktSendData.pkt.(*packet.PublishPacket)
				pktID = pubPkt.PacketID
			}
			// Get the packet ID, if any, for errors
			resultCh := pktSendData.resultCh
			if conn.verifyPacketType(pktSendData.pkt.Type()) != queuedPacketSendType {
				resultCh <- MqttResponse{
					Err: fmt.Errorf("Packet type must be sent directly"),
				}
			} else {
				err := conn.writePacket(pktSendData.pkt)
				if err != nil {
					// If there was an error sending, we can notify the caller
					// immediately.
					resultCh <- MqttResponse{
						PacketID: pktID,
						Err:      err,
					}
				}
			}
		}
	}
}

// Returns the channel to send the response, and the response data. If this
// gets complicated, we can handle each type separately
func (conn *mqttConn) createResponseForPacket(p packet.Packet) MqttResponse {
	switch p.Type() {
	case packet.PINGRESP:
		// successful ping response is just nil errors
		return MqttResponse{Err: nil}
	case packet.PUBACK:
		pubAck := p.(*packet.PubackPacket)
		return MqttResponse{PacketID: pubAck.PacketID, Err: nil}
	case packet.CONNACK:
		connAck := p.(*packet.ConnackPacket)
		var err error = nil
		if connAck.ReturnCode != packet.ConnectionAccepted {
			err = connAck.ReturnCode
		}
		return MqttResponse{Err: err}
	case packet.SUBACK:
		subAck := p.(*packet.SubackPacket)
		resp := MqttResponse{
			// We have do asynchronous SUBSCRIPTIONS, so our packets won't
			// have packet ID's. But, if we ever do them, this is one place that
			// we won't have to change our code.
			PacketID: subAck.PacketID,
		}
		for i, code := range subAck.ReturnCodes {
			if code == packet.QOSFailure {
				err := errors.New("subscription rejected")
				if i == 0 {
					// If someone just checks Err of the response, at least
					// they'll know that there was a failur
					resp.Errs = append(resp.Errs, err)
				}
			}
		}
		return resp
	default:
		logError("Unhandled packet type for response: ", p.Type())
		return MqttResponse{}
	}
}

func (conn *mqttConn) runPacketReader(wg *sync.WaitGroup) {
	defer func() {
		logInfo("[MQTT] packet reader is exiting")
		wg.Done()
	}()

	for {
		p, err := conn.stream.Read()
		conn.lastActivity = time.Now()
		if err != nil {
			// Disconnect "responses" are EOF
			if err == io.EOF {
				// Server disconnected. This happens for 2 reasons:
				// 1. We initiated a disconnect
				// 2. we don't ping, so the server assumed we're done
				logInfo("[MQTT] server disconnected: %s", err.Error())
			} else {
				logError("[MQTT] failed to read packet: %s", err.Error())
			}
			conn.status = DisconnectedNetworkStatus
			// The signal to the caller that disconnect was complete is the
			// exiting of this function (and wg.Done())
			break
		}
		if p.Type() == packet.PUBLISH {
			// Incoming publish, received from our subscription.
			pubPkt := p.(*packet.PublishPacket)
			conn.PubHandler.handlePubReceive(pubPkt)
		} else {
			// Everything besides publish and disconnect, so unpackage the
			// packet data and send it to the appropriate channel
			respData := conn.createResponseForPacket(p)
			respCh := conn.getResponseChannel(p.Type())
			select {
			case respCh <- respData:
			default:
				logError("Response channel is full. Dropping return packets")
			}
		}
	}
}

func (conn *mqttConn) writePacket(p packet.Packet) error {
	conn.lastActivity = time.Now()
	// XXX - I've used a SetWriteDeadline() for this, even on Flush, but I've
	// never gotten the write's to timeout. I think it's because the underlying
	// stream is buffered. It still doesn't quite make sense, because Flush() on
	// the buffered stream still forces a Write on the unbuffered Writer. My
	// guess is that it's the nature of TCP. If there's no failure, even the
	// lack of an ACK on write won't result in timing out. But, in any case, we
	// will still have a response timeout on the round trip, which might be
	// sufficient.
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

func (conn *mqttConn) getPacketID() uint16 {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	// If we were strictly incrementing, we could use atomic.AddUint32(), but
	// we're also wrapping around, so we still need the mutex.
	conn.lastPacketID += 1
	if conn.lastPacketID == 0 {
		conn.lastPacketID = 1
	}
	return conn.lastPacketID
}

// Sanity check to verify that we are queue'ing or non-queue'ing the correct
// types of packets. ACK's are not queued, of course, but since we can use this
// function to make route returns, we handle ACK's in this function too.
func (conn *mqttConn) verifyPacketType(pktType packet.Type) packetSendType {
	switch pktType {
	case packet.CONNECT, packet.CONNACK, packet.DISCONNECT, packet.SUBSCRIBE,
		packet.SUBACK:
		return directPacketSendType
	case packet.PUBLISH, packet.PUBACK, packet.PINGREQ, packet.PINGRESP:
		return queuedPacketSendType
	default:
		//
		logError("[MQTT] Unhandled packet type: %s", pktType)
		return unhandledPacketSendType
	}
}

func (conn *mqttConn) getResponseChannel(pktType packet.Type) chan MqttResponse {
	switch pktType {
	case packet.CONNECT, packet.CONNACK:
		return conn.connRespCh
	case packet.SUBSCRIBE, packet.SUBACK:
		return conn.subRespCh
	case packet.PUBLISH, packet.PUBACK:
		return conn.queueAckCh
	case packet.PINGREQ, packet.PINGRESP:
		return conn.pingRespCh
	case packet.DISCONNECT:
		return nil
	default:
		logError("[MQTT] Unhandled packet type: %s", pktType)
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
