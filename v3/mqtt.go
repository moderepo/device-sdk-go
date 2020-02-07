// Package Mode implements the Mode Client MQTT API
// The interface is through the MqttClient struct, which supports the MQTT
// subset that is required for our devices and configuration is through the
// MqttDelegate.
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
	// There is currently an active connection to the server
	ConnectedNetworkStatus NetworkStatus = iota
	// We have successfully disconnected to the server
	DisconnectedNetworkStatus
	// If we have had requests time out, we set to timing out. We should
	// reconnect.
	TimingOutNetworkStatus
	// Not yet connected state
	DefaultNetworkStatus
)

type (
	// MqttSubData to send in the channel to the client when we receive data
	// published for our subscription.
	MqttSubData struct {
		topic string
		data  []byte
	}

	// MqttResponse is result of an MQTT Request. Not all of these members will
	// be valid. For example, only PUBACK's will have PacketIds, and PUBLISH'es
	// will send subscription data. In some cases, there will be multiple
	// errors, so the Errs slice will be populated rather than the Errs.
	MqttResponse struct {
		PacketID uint16
		Err      error
		Errs     []error
	}

	// MqttDelegate should be implemented by users of MqttClient. Three
	// responsibilities (Oops. single responsibility, blah blah.):
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

		// SetReceiveChannels will be called by the MqttClient. The MqttClient
		// will create the channels with the buffer size returned by
		// GetReceieveQueueSize(). The implementor of the delegate will use
		// these channels to receive information from the server, such as
		// queued responses and subscriptions:
		// subRecvCh: Data published from our subscriptions.
		// queueAckCh: API requests that are queued will receive MqttPublishID's
		// which will be ACK'ed. The MqttQueueResult will have the
		// MqttPublishID and the result
		// pingAckCh: True if our ping received an ACK or false if timeout
		SetReceiveChannels(subRecvCh <-chan MqttSubData,
			queueAckCh <-chan MqttResponse,
			pingAckCh <-chan MqttResponse)

		// Buffer size of the incoming queues to the delegate. This is the
		// size of the three receive channels
		GetReceiveQueueSize() uint16

		// Buffer size of the outgoing queue to the server. This cannot be
		// changed after a connection is created
		GetSendQueueSize() uint16

		// Retrieve the topics for this delegate
		GetSubscriptions() []string

		// Hook so we can clean up on closing of connections
		OnClose()
	}

	// MqttClient provides the public API to MQTT. We handle
	// connect, disconnect, ping, publish, and subscribe.
	// Connect, disconnect, and subscribe will block and wait for the
	// response. Ping and publish will return after the packet has been
	// sent and the response will be sent on a channel that is provided
	// by the delegate. For ping, since MQTT does not provide a mechanism
	// to distinguish between different ping requests, we do not provide
	// an API to distinguish them either. For publish, the function returns
	// a packet ID. This packet ID will be returned to the caller in the
	// channel.
	MqttClient struct {
		mqttHost  string
		mqttPort  int
		delegate  MqttDelegate
		conn      *mqttConn
		wgSend    sync.WaitGroup // wait on sending
		wgRecv    sync.WaitGroup // wait on receiving
		lastError error

		delegateSubRecvCh chan MqttSubData
	}

	// Delegate for the mqqtConn to call back to the MqttClient
	mqttReceiver interface {
		// Called by the connection when receiving publishes
		handlePubReceive(*packet.PublishPacket)
		// Called by the connection when there is an error
		setError(error)
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
		// delegate to handle receiving asynchronous events from the server.
		// This is more explicit than a channel, but it has the drawback
		// of just being a functional call. So, the implementation should
		// consist of routing (which it is, since it is just a callback
		// into the MqttClient). If circumstances change, change, we can
		// revisit this decision.
		Receiver mqttReceiver

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

// NewMqttClient will create client and open a stream. A client is invalid if
// not connected, and you need to create a new client to reconnect.
func NewMqttClient(mqttHost string, mqttPort int,
	delegate MqttDelegate) *MqttClient {
	return &MqttClient{
		mqttHost: mqttHost,
		mqttPort: mqttPort,
		delegate: delegate,
	}

}

// IsConnected will return true if we have a successfully CONNACK'ed response.
//
func (client *MqttClient) IsConnected() bool {
	return client.conn.status == ConnectedNetworkStatus
}

// GetLastActivity will return the time since the last send or
// receive.
func (client *MqttClient) GetLastActivity() time.Time {
	return client.conn.lastActivity
}

// Connect will initiate a connection to the server. It will block until we
// receive a CONNACK from the server.
func (client *MqttClient) Connect(ctx context.Context) error {
	if client.conn != nil {
		return errors.New("Cannot connect when already connect")
	}
	client.createMqttConnection()
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

// Disconnect will end this connection with the server. We will block until
// the server closes the connection.
// Note: We might want to wait, but order is important here.
func (client *MqttClient) Disconnect(ctx context.Context) error {
	defer func() {
		client.shutdownConnection()
		close(client.delegateSubRecvCh)
		client.delegate.OnClose()
	}()

	// Maybe we want to add a Connecting/Disconnecting status?
	client.conn.status = DefaultNetworkStatus
	p := packet.NewDisconnectPacket()
	_, err := client.conn.sendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	return nil
}

// Subscribe will query the delegate for its subscriptions via the
// GetSubscriptions method. This is a synchronous call so it will block until
// a response is received from the server. It will return a slice of errors
// which will be in the same order as the subscriptions in GetSubscriptions().
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

// Ping sends an MQTT PINGREQ event to the server. This is an asynchronous
// call, so we will always return success if we were able to queue the message
// for delivery. Any subsequent results will be sent on the delegate's
// pingAckCh.
func (client *MqttClient) Ping(ctx context.Context) error {

	p := packet.NewPingreqPacket()
	_, err := client.conn.queuePacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	return nil
}

// Publish sends an MQTT Publish event to subscribers on the specified
// topic. This is an asynchronous call, so we will always return a packet
// ID as long as the request is able to be queued. After queueing, the
// any subsequent errors or results will be written to the delegate's
// queueAckCh.
// For QOSAtMostOnce, there will only be an error returned if the request was
// unable to be queued. We receive no ACK from the server.
// For QOSAtLeastOnce, we will receive an ACK if we were successful.
// For any other QOS levels (QOSExactlyOnce), they are not supported and an
// error is returned immediately and the request will not be sent.
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
	logInfo("[MQTT] Publishing on topic [%s]", p.Message.Topic)

	return client.conn.queuePacket(ctx, p)
}

// Each connect, we need to create a new mqttConnection.
func (client *MqttClient) createMqttConnection() {
	receiveQueueSize := client.delegate.GetReceiveQueueSize()
	subRecvCh := make(chan MqttSubData, receiveQueueSize)
	queueAckCh := make(chan MqttResponse, receiveQueueSize)
	pingAckCh := make(chan MqttResponse, receiveQueueSize)
	client.delegate.SetReceiveChannels(subRecvCh, queueAckCh, pingAckCh)
	client.delegateSubRecvCh = subRecvCh
	useTLS, tlsConfig := client.delegate.TLSUsageAndConfiguration()
	sendQueueSize := client.delegate.GetSendQueueSize()
	// XXX - we pass quit a few arguments to the connection. Maybe we should
	// pass in the delegate instead. This allows the delegate to provide the
	// connection to give data to dynamically
	conn := newMqttConn(tlsConfig, client.mqttHost, client.mqttPort, useTLS,
		queueAckCh, pingAckCh, sendQueueSize)

	client.conn = conn
	conn.Receiver = client

	// We want to pass our WaitGroup's to the connection reader and writer, so
	// we don't put these in the mqttConn's constructor.
	client.wgSend.Add(1)
	go conn.runPacketWriter(&client.wgSend)
	client.wgRecv.Add(1)
	go conn.runPacketReader(&client.wgRecv)
}

// Shutting down gracefully is tricky. But, we try our best to drain the channels
// and avoid any panics.
func (client *MqttClient) shutdownConnection() {
	// We skip encapsulation of the connection class so the steps are clear
	// 1. Close the channels to stop writing. This will break the
	//    packetWriter goroutine out of its loop
	close(client.conn.directSendPktCh)
	close(client.conn.queuedSendPktCh)
	// 2. wait for packet sender to finish
	client.wgSend.Wait()
	// 3. Close the connection with the server. This will break the
	//    packet reader out of its loop. Set to disconnected here so their
	//    reader knows that it was not an error
	client.conn.status = DisconnectedNetworkStatus
	client.conn.conn.Close()
	// 4. Wait for the packet reader to finish
	client.wgRecv.Wait()
	// 5. Close the channels for handling responses
	close(client.conn.connRespCh)
	close(client.conn.subRespCh)
	close(client.conn.queueAckCh)
	close(client.conn.pingRespCh)
	client.conn = nil
}

// Helper function called by the synchronous API to handle processing
// of the responses. For the asynchronous API, the caller might do something
// similar, but also handling packet ID's.
func (client *MqttClient) receivePacket(ctx context.Context,
	respChan chan MqttResponse) MqttResponse {
	defer func() {
		client.wgRecv.Done()
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

// Implementation of the delegate for mqttConn to handle publish from the
// server on topics that we've subscribed to. We only unpack it and send it
// to the caller
func (client *MqttClient) handlePubReceive(p *packet.PublishPacket) {
	logInfo("[MQTT] received message for topic %s", p.Message.Topic)

	pubData := MqttSubData{p.Message.Topic, p.Message.Payload}

	select {
	case client.delegateSubRecvCh <- pubData:
	default:
		logError("Caller could not receive publish data. SubRecCh full?")
		client.lastError = errors.New("SubRecCh channel is full")
	}
}

func (client *MqttClient) setError(err error) {
	client.lastError = err
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
		// blocking, queue size specified by the delegate. If it is insufficient,
		// we will lose packets.
		queuedSendPktCh: make(chan packetSendData, outgoingQueueSize),

		// Connection requests are blocking, so no buffer
		connRespCh: make(chan MqttResponse),
		subRespCh:  make(chan MqttResponse),

		// These are passed to us by the client, with a buffer sized specified
		// by the delegate, so it is the delegate's responsibility to set the
		// size appropriately or we will start losing responses.
		pingRespCh: pingAckCh,
		queueAckCh: queueAckCh,
	}
}

// We only queue pings (PINGREQ) and publishes (PUBLISH). Theoretically, we
// could queue subscribes (SUBSCRIBE) since they have packet ID's like
// publishes. But, for simplicity, those are synchronous, since, in practice,
// those are either on startup, or, at least, on rare occasions.
func (conn *mqttConn) queuePacket(ctx context.Context,
	p packet.Packet) (uint16, error) {
	if conn.status == DisconnectedNetworkStatus ||
		conn.status == TimingOutNetworkStatus {
		return 0, errors.New("Connection unstable. Unable to send")
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
		return nil, errors.New("Connection unstable. Unable to send")
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
				// Case where the channel is closed
				shouldLoop = false
				break
			}
			if conn.verifyPacketType(pktSendData.pkt.Type()) != directPacketSendType {
				resultCh <- MqttResponse{
					Err: errors.New("Packet type must be queued"),
				}
			} else {
				resultCh <- MqttResponse{Err: conn.writePacket(pktSendData.pkt)}
			}
		case pktSendData := <-conn.queuedSendPktCh:
			if pktSendData.pkt == nil {
				// Case where the channel is closed
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
					Err: errors.New("Packet type must be sent directly"),
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
		// Set a deadline for reads to prevent blocking forever. We'll handle
		// this error and continue looping, if appropriate
		conn.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 500))
		p, err := conn.stream.Read()
		conn.lastActivity = time.Now()
		if err != nil {
			// Disconnect "responses" are EOF
			if err == io.EOF || conn.status == DisconnectedNetworkStatus {
				// Server disconnected. This happens for 2 reasons:
				// 1. We initiated a disconnect
				// 2. we don't ping, so the server assumed we're done
				logInfo("[MQTT] net.Conn disconnected: %s", err.Error())
			} else {
				// I/O Errors usually return this, so if it is, we can
				// figure out what to do next
				opError := err.(*net.OpError)
				if opError != nil {
					if os.IsTimeout(opError.Err) {
						// 	// No problem - read deadline just exceeded
						continue
					}
				}
				logError("[MQTT] failed to read packet: %s", err.Error())
			}
			// The signal to the caller that disconnect was complete is the
			// exiting of this function (and wg.Done())
			break
		}
		if p.Type() == packet.PUBLISH {
			// Incoming publish, received from our subscription.
			pubPkt := p.(*packet.PublishPacket)
			conn.Receiver.handlePubReceive(pubPkt)
		} else {
			// Everything besides publish and disconnect, so unpackage the
			// packet data and send it to the appropriate channel
			respData := conn.createResponseForPacket(p)
			respCh := conn.getResponseChannel(p.Type())
			logInfo("[MQTT] Received %s", p.Type())
			select {
			case respCh <- respData:
				logInfo("[MQTT] Sent %s", p.Type())
			default:
				logError("[MQTT] Response channel is full. Dropping return packets")
				conn.Receiver.setError(errors.New("Response channel full. Dropping packet"))
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
	conn.lastPacketID++
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
