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

	mqttpacket "github.com/moderepo/device-sdk-go/v4/internal/packet"
	"github.com/moderepo/device-sdk-go/v4/internal/websocket"
)

const (
	mqttConnectTimeout   = time.Second * 10
	connResponseDeadline = time.Second * 10
)

var mqttDialer = &net.Dialer{Timeout: mqttConnectTimeout}

// QOSLevel is the QoS level of message delivery. This is used in sending events to MODE.
type QOSLevel int

const (
	// QOSAtMostOnce means QoS 0 - message delivery is not guaranteed.
	QOSAtMostOnce QOSLevel = iota

	// QOSAtLeastOnce means QoS 1 - message is delivered at least once, but duplicates may happen.
	QOSAtLeastOnce

	// QOSExactlyOnce means QoS 2 - message is always delivered exactly once. This is currently not supported.
	QOSExactlyOnce
)

type packetSendType int

const (
	// Packets are written out to the stream immediately (or after the
	// previous packet has finished sending)
	directPacketSendType packetSendType = iota
	// Packets are queued and written when processed
	queuedPacketSendType
	unhandledPacketSendType
)

type NetworkStatus int

const (
	// ConnectedNetworkStatus - There is currently an active connection to the server
	ConnectedNetworkStatus NetworkStatus = iota
	// DisconnectedNetworkStatus - We have successfully disconnected to the server
	DisconnectedNetworkStatus
	// TimingOutNetworkStatus - If we have had requests time out, we set to timing out. We should reconnect.
	TimingOutNetworkStatus
	// DefaultNetworkStatus - Not yet connected state
	DefaultNetworkStatus
)

type (
	// MqttSubData to send in the channel to the client when we receive data
	// published for our subscription.
	MqttSubData struct {
		Topic       string
		Data        []byte
		ReceiveTime time.Time
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

	// MqttAuthDelegate methods provide the security and authentication
	// information to start a connection to the MQTT server
	MqttAuthDelegate interface {
		// AuthInfo returns authentication information
		AuthInfo() (username string, password string)
		// UseTLS indicates whether TLS should be used to connect to the server.
		UseTLS() bool
	}

	// MqttReceiverDelegate methods allow the MqttClient to communicate
	// information and events back to the user.
	MqttReceiverDelegate interface {
		// SetReceiveChannels will be called by the MqttClient. The MqttClient
		// will create the channels with the buffer size returned by
		// MqttConfigDelegate.GetReceiveQueueSize. The implementor of the delegate will use
		// these channels to receive information from the server, such as
		// queued responses and subscriptions:
		// subRecvCh: Data published from our subscriptions.
		// queueAckCh: API requests that are queued will receive MqttPublishID's
		// which will be ACK'ed. The MqttQueueResult will have the
		// MqttPublishID and the result
		// pingAckCh: True if our ping received an ACK or false if timeout
		// Note: These channels will be closed when the connection is closed (from
		// a Disconnect), so the user should stop listening to these channels when
		// OnClose() is called..
		SetReceiveChannels(subRecvCh <-chan MqttSubData,
			queueAckCh <-chan MqttResponse,
			pingAckCh <-chan MqttResponse)

		// OnClose is a hook for cleaning up on closing of connections
		OnClose()
	}

	// MqttConfigDelegate methods allow the MqttClient to configure itself
	// according to the requirements of the user
	MqttConfigDelegate interface {
		// GetReceiveQueueSize returns the buffer size of the incoming queues to the delegate. This is the
		// size of the three receive channels
		GetReceiveQueueSize() uint16

		// GetSendQueueSize returns the buffer size of the outgoing queue to the server. This cannot be
		// changed after a connection is created
		GetSendQueueSize() uint16
	}

	// MqttErrorDelegate is an optional delegate which allows the MqttClient
	// a method of signaling errors that cannot be communicated
	// through the normal channels. See handling errors in the documentation.
	MqttErrorDelegate interface {
		// GetErrorChannelSize returns the buffer size of the error channel
		GetErrorChannelSize() uint16

		// SetErrorChannel provides the delegate the channel to receive errors
		SetErrorChannel(errCh chan error)
	}

	// MqttDelegate is the combined required interfaces that must be implemented
	// to use the MqttClient. This is a convenience that the user can use to
	// allow a single struct to implement all the required interfaces
	MqttDelegate interface {
		MqttAuthDelegate
		MqttReceiverDelegate
		MqttConfigDelegate
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
		mqttHost      string
		mqttPort      int
		useWebSocket  bool
		authDelegate  MqttAuthDelegate
		recvDelegate  MqttReceiverDelegate
		confDelegate  MqttConfigDelegate
		errorDelegate MqttErrorDelegate
		conn          *mqttConn
		wgSend        sync.WaitGroup
		stopWriterCh  chan struct{}
		wgRecv        sync.WaitGroup
		lastErrors    []error

		delegateSubRecvCh chan MqttSubData

		connMtx  sync.Mutex
		errorMtx sync.Mutex
	}

	// Delegate for the mqttConn to call back to the MqttClient
	mqttReceiver interface {
		// Called by the connection when receiving publishes
		handlePubReceive(pkt *mqttpacket.PublishPacket, receiveTime time.Time)
		// Called by the connection when there is an error
		appendError(error)
	}

	packetSendData struct {
		pkt      mqttpacket.Packet
		resultCh chan<- MqttResponse
	}

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
		stream *mqttpacket.Stream
		// Sequential packet ID. Used to match to ack our actions (pub)
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
		connRespCh  chan MqttResponse
		subRespCh   chan MqttResponse
		unsubRespCh chan MqttResponse
		pingRespCh  chan MqttResponse
		queueAckCh  chan MqttResponse
		// This is optional and may be nil
		errCh chan error

		mutex             sync.Mutex
		statusMutex       sync.RWMutex
		lastActivityMutex sync.RWMutex
	}
)

func WithMqttAuthDelegate(authDelegate MqttAuthDelegate) func(*MqttClient) {
	return func(c *MqttClient) {
		c.authDelegate = authDelegate
	}
}

func WithMqttReceiverDelegate(recvDelegate MqttReceiverDelegate) func(*MqttClient) {
	return func(c *MqttClient) {
		c.recvDelegate = recvDelegate
	}
}

func WithMqttConfigDelegate(confDelegate MqttConfigDelegate) func(*MqttClient) {
	return func(c *MqttClient) {
		c.confDelegate = confDelegate
	}
}

func WithMqttErrorDelegate(errorDelegate MqttErrorDelegate) func(*MqttClient) {
	return func(c *MqttClient) {
		c.errorDelegate = errorDelegate
	}
}

func WithMqttDelegate(delegate MqttDelegate) func(*MqttClient) {
	return func(c *MqttClient) {
		c.authDelegate = delegate
		c.recvDelegate = delegate
		c.confDelegate = delegate
	}
}

func WithUseWebSocket(b bool) func(client *MqttClient) {
	return func(c *MqttClient) {
		c.useWebSocket = b
	}
}

// NewMqttClient will create client and open a stream. A client is invalid if
// not connected, and you need to create a new client to reconnect.
func NewMqttClient(mqttHost string, mqttPort int,
	dels ...func(*MqttClient)) *MqttClient {
	client := &MqttClient{
		mqttHost:   mqttHost,
		mqttPort:   mqttPort,
		lastErrors: make([]error, 0, 5),
	}

	for _, del := range dels {
		del(client)
	}

	if client.authDelegate != nil && client.recvDelegate != nil &&
		client.confDelegate != nil {
		return client
	}

	return nil
}

// IsConnected will return true if we have a successfully CONNACK'ed response.
func (client *MqttClient) IsConnected() bool {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()
	return client.conn != nil && client.conn.getStatus() == ConnectedNetworkStatus
}

func (client *MqttClient) setConn(c *mqttConn) {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()
	client.conn = c
}

func (client *MqttClient) getConn() *mqttConn {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()
	return client.conn
}

// GetLastActivity will return the time since the last send or
// receive.
func (client *MqttClient) GetLastActivity() time.Time {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()
	return client.conn.GetLastActivity()
}

func (client *MqttClient) sendPacket(ctx context.Context,
	p mqttpacket.Packet) (chan MqttResponse, error) {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()
	return client.conn.sendPacket(ctx, p)
}

func (client *MqttClient) setStatus(status NetworkStatus) {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()
	client.conn.setStatus(status)
}

func (client *MqttClient) getStatus() NetworkStatus {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()
	return client.conn.getStatus()
}

func (client *MqttClient) getPacketID() uint16 {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()
	return client.conn.getPacketID()
}

func (client *MqttClient) queuePacket(ctx context.Context,
	p mqttpacket.Packet) (uint16, error) {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()

	return client.conn.queuePacket(ctx, p)
}
func (client *MqttClient) sendQueueingError(err error) {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()

	client.conn.sendQueueingError(err)
}

// Connect will initiate a connection to the server. It will block until we
// receive a CONNACK from the server.
func (client *MqttClient) Connect(ctx context.Context) error {
	if client.getConn() != nil {
		return errors.New("cannot connect when already connected")
	}
	if err := client.createMqttConnection(); err != nil {
		return err
	}

	user, pwd := client.authDelegate.AuthInfo()
	p := mqttpacket.NewConnectPacket()
	p.Version = mqttpacket.Version311
	p.Username = user
	p.Password = pwd
	p.CleanSession = true

	respChan, err := client.sendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	client.wgRecv.Add(1)
	resp := client.receivePacket(ctx, respChan)
	if resp.Err != nil {
		client.setStatus(DisconnectedNetworkStatus)
		return resp.Err
	}
	// If we made it here, we consider ourselves connected
	client.setStatus(ConnectedNetworkStatus)

	return nil
}

// Disconnect will end this connection with the server. We will block until
// the server closes the connection.
// Note: We might want to wait, but order is important here.
func (client *MqttClient) Disconnect(ctx context.Context) error {
	if client.getConn() == nil {
		// nothing to disconnect. Return
		return nil
	}

	defer func() {
		client.shutdownConnection()
	}()

	// Maybe we want to add a Connecting/Disconnecting status?
	client.setStatus(DefaultNetworkStatus)
	p := mqttpacket.NewDisconnectPacket()
	_, err := client.sendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	return nil
}

// Subscribe will subscribe to the topics in subs by sending a subscribe
// request. This is a synchronous call so it will block until
// a response is received from the server. It will return a slice of errors
// which will be in the same order as the subscriptions in Subscriptions().
func (client *MqttClient) Subscribe(ctx context.Context,
	subs []string) []error {
	p := mqttpacket.NewSubscribePacket()
	p.Subscriptions = make([]mqttpacket.Subscription, 0, 10)
	p.PacketID = client.getPacketID()

	for _, sub := range subs {
		// We have no protection to keep you from subscribing to the same
		// topic multiple times. Maybe we should? Maybe the server would send
		// us 2 messages then for each topic?
		logInfo("[MQTT] subscribing to topic %s", sub)
		p.Subscriptions = append(p.Subscriptions, mqttpacket.Subscription{
			Topic: sub,
			// MODE only supports QoS0 for subscriptions
			QOS: mqttpacket.QOSAtMostOnce,
		})
	}

	respChan, err := client.sendPacket(ctx, p)
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

// Unsubscribe will send an unsubscribe request for the topics in subs.
// This is a synchronous call.
func (client *MqttClient) Unsubscribe(ctx context.Context,
	subs []string) []error {
	p := mqttpacket.NewUnsubscribePacket()
	p.Topics = subs
	p.PacketID = client.getPacketID()

	respChan, err := client.sendPacket(ctx, p)
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
// for delivery. Results will be sent on the delegate's pingAckCh
func (client *MqttClient) Ping(ctx context.Context) error {

	p := mqttpacket.NewPingreqPacket()
	_, err := client.queuePacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	return nil
}

// PingAndWait sends an MQTT PINGREQ event to the server and waits for the
// response. If this method is used instead of the asynchronous Ping, user
// should not be listening on the pingAckCh channel since this function may
// time out waiting for the response an error will be returned.
func (client *MqttClient) PingAndWait(ctx context.Context) error {

	p := mqttpacket.NewPingreqPacket()
	respChan, err := client.sendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	client.wgRecv.Add(1)
	resp := client.receivePacket(ctx, respChan)
	return resp.Err
}

// Publish sends an MQTT Publish event to subscribers on the specified
// topic. This is an asynchronous call, so we will always return a packet
// ID as long as the request can be queued. After queueing,
// any subsequent errors or results will be written to the delegate's
// queueAckCh.
// For QOSAtMostOnce, there will only be an error returned if the request was
// unable to be queued. We receive no ACK from the server.
// For QOSAtLeastOnce, we will receive an ACK if we were successful.
// For any other QOS levels (QOSExactlyOnce), they are not supported and an
// error is returned immediately and the request will not be sent.
func (client *MqttClient) Publish(ctx context.Context, qos QOSLevel,
	topic string, data []byte) (uint16, error) {
	return client.publishWithID(ctx, qos, topic, data, 0)
}

func (client *MqttClient) Republish(ctx context.Context, qos QOSLevel,
	topic string, data []byte, packetID uint16) (uint16, error) {
	return client.publishWithID(ctx, qos, topic, data, packetID)
}

func (client *MqttClient) publishWithID(ctx context.Context, qos QOSLevel,
	topic string, data []byte, packetID uint16) (uint16, error) {
	var pktQos byte
	switch qos {
	case QOSAtMostOnce:
		pktQos = mqttpacket.QOSAtMostOnce
	case QOSAtLeastOnce:
		pktQos = mqttpacket.QOSAtLeastOnce
	default:
		return 0, errors.New("unsupported qos level")
	}

	p := mqttpacket.NewPublishPacket()
	p.PacketID = packetID
	p.Message = mqttpacket.Message{
		Topic:   topic,
		QOS:     pktQos,
		Payload: data,
	}

	return client.queuePacket(ctx, p)
}

// Each connect, we need to create a new mqttConnection.
func (client *MqttClient) createMqttConnection() error {
	receiveQueueSize := client.confDelegate.GetReceiveQueueSize()
	subRecvCh := make(chan MqttSubData, receiveQueueSize)
	queueAckCh := make(chan MqttResponse, receiveQueueSize)
	pingAckCh := make(chan MqttResponse, receiveQueueSize)
	client.recvDelegate.SetReceiveChannels(subRecvCh, queueAckCh, pingAckCh)
	client.delegateSubRecvCh = subRecvCh
	useTLS := client.authDelegate.UseTLS()
	sendQueueSize := client.confDelegate.GetSendQueueSize()
	conn := newMqttConn(client.mqttHost, client.mqttPort, useTLS, client.useWebSocket,
		queueAckCh, pingAckCh, sendQueueSize)

	if conn == nil {
		return errors.New("unable to create a socket to the server")
	}

	if client.errorDelegate != nil {
		errCh := make(chan error, client.errorDelegate.GetErrorChannelSize())
		client.errorDelegate.SetErrorChannel(errCh)
		conn.errCh = errCh
	}

	client.setConn(conn)
	conn.Receiver = client

	// We want to pass our WaitGroup's to the connection reader and writer, so
	// we don't put these in the mqttConn's constructor.
	client.stopWriterCh = make(chan struct{})
	client.wgSend.Add(1)
	go conn.runPacketWriter(client.stopWriterCh, &client.wgSend)
	client.wgRecv.Add(1)
	go conn.runPacketReader(&client.wgRecv)

	return nil
}

// Shutting down gracefully is tricky. But, we try our best to drain the channels
// and avoid any panics.
func (client *MqttClient) shutdownConnection() {
	client.connMtx.Lock()
	defer client.connMtx.Unlock()

	// We skip encapsulation of the connection class so the steps are clear
	// 1. Send close to the packetWriter goroutine
	client.stopWriterCh <- struct{}{}
	// 2. Wait until the done channel has been read, since there are some queued
	// writes that might be sent before the done channel has been read.
	client.wgSend.Wait()
	// 3. Close the channels writer channels
	close(client.conn.directSendPktCh)
	close(client.conn.queuedSendPktCh)
	// 4. Close the connection with the server. This will break the
	//    packet reader out of its loop. Set to disconnected here so their
	//    reader knows that it was not an error
	client.conn.setStatus(DisconnectedNetworkStatus)
	_ = client.conn.conn.Close()
	// 5. Wait for the packet reader to finish
	client.wgRecv.Wait()
	// 6. Notify the client that we are disconnecting
	client.recvDelegate.OnClose()
	// 6. Close the channels for handling responses
	close(client.conn.connRespCh)
	close(client.conn.subRespCh)
	// 7. Close the channel to the client readers.
	close(client.conn.queueAckCh)
	close(client.conn.pingRespCh)
	close(client.delegateSubRecvCh)
	if client.conn.errCh != nil {
		close(client.conn.errCh)
	}
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
func (client *MqttClient) handlePubReceive(p *mqttpacket.PublishPacket,
	receiveTime time.Time) {
	logInfo("[MQTT] received message for topic %s", p.Message.Topic)

	pubData := MqttSubData{
		Topic:       p.Message.Topic,
		Data:        p.Message.Payload,
		ReceiveTime: receiveTime,
	}

	// If we have successfully queued the pub to the user
	select {
	case client.delegateSubRecvCh <- pubData:
	default:
		logError("Caller could not receive publish data. SubRecCh full?")
		client.sendQueueingError(nil)
		return
	}
	if p.Message.QOS != mqttpacket.QOSAtMostOnce {
		ackPkt := mqttpacket.NewPubackPacket()
		ackPkt.PacketID = p.PacketID
		// For everything except QOS2: if we have queued the message to the
		// user, we consider that successfully published to us, so we attempt
		// to send an ACK back to the server.
		ctx, cancel := context.WithTimeout(context.Background(),
			connResponseDeadline)
		defer cancel()
		if _, err := client.queuePacket(ctx, ackPkt); err != nil {
			logError("[MQTT] Queueing error on handlePubReceive %+v", err)
			client.sendQueueingError(err)
		}
	}
}

// TakeRemainingErrors returns the last errors, and then resets the errors. If there is no
// error delegate or the error delegate's error channel is full, we "queue"
// errors in a slice that can be fetched.
func (client *MqttClient) TakeRemainingErrors() []error {
	client.errorMtx.Lock()
	defer client.errorMtx.Unlock()
	defer func() {
		client.lastErrors = make([]error, 0, 5)
	}()
	return client.lastErrors
}

func (client *MqttClient) appendError(err error) {
	client.errorMtx.Lock()
	defer client.errorMtx.Unlock()
	client.lastErrors = append(client.lastErrors, err)
}

func newMqttConn(mqttHost string, mqttPort int, useTLS, useWebSocket bool,
	queueAckCh chan MqttResponse, pingAckCh chan MqttResponse, outgoingQueueSize uint16) *mqttConn {

	addr := fmt.Sprintf("%s:%d", mqttHost, mqttPort)
	var conn net.Conn
	var err error

	if useWebSocket {
		network := "wss"
		if !useTLS {
			network = "ws"
		}
		if conn, err = websocket.Dial(network, addr); err != nil {
			logError("WebSocket dialer failed: %s", err.Error())
			return nil
		}
	} else if useTLS {
		if conn, err = tls.DialWithDialer(mqttDialer, "tcp", addr, nil); err != nil {
			logError("MQTT TLS dialer failed: %s", err.Error())
			return nil
		}
	} else {
		if conn, err = mqttDialer.Dial("tcp", addr); err != nil {
			logError("MQTT dialer failed: %s", err.Error())
			return nil
		}
	}

	return &mqttConn{
		conn:   conn,
		stream: mqttpacket.NewStream(conn, conn),
		status: DefaultNetworkStatus,

		// The two channels that we write packets to send. The packetWriter
		// will listen on these and write out to the stream.
		// blocking, non-buffered
		directSendPktCh: make(chan packetSendData),
		// blocking, queue size specified by the delegate. If it is insufficient,
		// we will lose packets.
		queuedSendPktCh: make(chan packetSendData, outgoingQueueSize),

		// These are synchronous requests, so we send the packet, then wait for the response. But,
		// we create a buffer of 1, so if the response is received before we start listening
		// to this channel, it won't be dropped
		connRespCh:  make(chan MqttResponse, 1),
		subRespCh:   make(chan MqttResponse, 1),
		unsubRespCh: make(chan MqttResponse, 1),

		// These are passed to us by the client, with a buffer sized specified
		// by the delegate, so it is the delegate's responsibility to set the
		// size appropriately, or we will start losing responses.
		pingRespCh: pingAckCh,
		queueAckCh: queueAckCh,
	}
}

func (conn *mqttConn) setStatus(status NetworkStatus) {
	conn.statusMutex.Lock()
	defer conn.statusMutex.Unlock()
	conn.status = status
}

func (conn *mqttConn) getStatus() NetworkStatus {
	conn.statusMutex.RLock()
	defer conn.statusMutex.RUnlock()
	return conn.status
}

func (conn *mqttConn) setLastActivity(t time.Time) {
	conn.lastActivityMutex.Lock()
	defer conn.lastActivityMutex.Unlock()
	conn.lastActivity = t
}

func (conn *mqttConn) GetLastActivity() time.Time {
	conn.lastActivityMutex.RLock()
	defer conn.lastActivityMutex.RUnlock()
	return conn.lastActivity
}

// We only queue pings (PINGREQ) and publishes (PUBLISH). Theoretically, we
// could queue subscribes (SUBSCRIBE) since they have packet ID's like
// publishes. But, for simplicity, those are synchronous, since, in practice,
// those are either on startup, or, at least, on rare occasions.
func (conn *mqttConn) queuePacket(ctx context.Context,
	p mqttpacket.Packet) (uint16, error) {
	if conn == nil || conn.getStatus() == DisconnectedNetworkStatus ||
		conn.getStatus() == TimingOutNetworkStatus {
		return 0, errors.New("connection unstable. unable to send")
	}

	packetID := uint16(0)
	if p.Type() == mqttpacket.PUBLISH {
		pubPkt := p.(*mqttpacket.PublishPacket)
		if pubPkt.PacketID != 0 {
			packetID = pubPkt.PacketID
		} else {
			packetID = conn.getPacketID()
			pubPkt.PacketID = packetID
		}
	}

	pktSendData := packetSendData{pkt: p,
		resultCh: conn.getResponseChannel(p.Type()),
	}

	select {
	// Put writing to the channel in a select because the buffer might be full
	case conn.queuedSendPktCh <- pktSendData:
		// Successfully sent, but we don't know when we'll get a response back,
		// so just don't do anything.
	case <-ctx.Done():
		logError("Exceeded timeout sending %s for id %d", p.Type(), packetID)
		return 0, fmt.Errorf("send queue full %s for id %d", p.Type(), packetID)
	}

	return packetID, nil
}

// Called by the client to send packets to the server
func (conn *mqttConn) sendPacket(ctx context.Context,
	p mqttpacket.Packet) (chan MqttResponse, error) {
	if conn == nil || conn.getStatus() == DisconnectedNetworkStatus ||
		conn.getStatus() == TimingOutNetworkStatus {
		return nil, errors.New("connection unstable. unable to send")
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
		return nil, errors.New("timeout error")
	}

	// Wait for the result, and then give that result back to the caller, who
	// will handle the error
	result := <-resultCh
	return conn.getResponseChannel(p.Type()), result.Err
}

func (conn *mqttConn) runPacketWriter(stopWriterCh chan struct{},
	wg *sync.WaitGroup) {
	defer func() {
		logInfo("[MQTT] packet writer is exiting")
		wg.Done()
	}()

	exitLoop := false
	for !exitLoop {
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
			resultCh <- MqttResponse{Err: conn.writePacket(pktSendData.pkt)}
		case pktSendData := <-conn.queuedSendPktCh:
			pktID := uint16(0)
			if pktSendData.pkt.Type() == mqttpacket.PUBLISH {
				pubPkt := pktSendData.pkt.(*mqttpacket.PublishPacket)
				pktID = pubPkt.PacketID
			}
			// Get the packet ID, if any, for errors. This is a long-lived channel, so
			// it's possible to be full.
			resultCh := pktSendData.resultCh
			err := conn.writePacket(pktSendData.pkt)
			if err != nil {
				// If there was an error sending, we can notify the caller
				// immediately.
				logError("[MQTT] Error occurred on runPacketWriter %v", err)
				select {
				case resultCh <- MqttResponse{
					PacketID: pktID,
					Err:      err,
				}:
				default:
					logError("[MQTT] Queueing error on runPacketWriter %+v", err)
					conn.sendQueueingError(err)
				}
			}
		case <-stopWriterCh:
			exitLoop = true
		}
	}
}

// Returns the channel to send the response, and the response data. If this
// gets complicated, we can handle each type separately
func (conn *mqttConn) createResponseForPacket(p mqttpacket.Packet) MqttResponse {
	switch p.Type() {
	case mqttpacket.PINGRESP:
		// successful ping response is just nil errors
		return MqttResponse{Err: nil}
	case mqttpacket.PUBACK:
		pubAck := p.(*mqttpacket.PubackPacket)
		return MqttResponse{PacketID: pubAck.PacketID, Err: nil}
	case mqttpacket.CONNACK:
		connAck := p.(*mqttpacket.ConnackPacket)
		var err error = nil
		if connAck.ReturnCode != mqttpacket.ConnectionAccepted {
			err = connAck.ReturnCode
		}
		return MqttResponse{Err: err}
	case mqttpacket.UNSUBACK:
		unsubAck := p.(*mqttpacket.UnsubackPacket)
		// There's only a packet ID.
		return MqttResponse{
			PacketID: unsubAck.PacketID}
	case mqttpacket.SUBACK:
		subAck := p.(*mqttpacket.SubackPacket)
		resp := MqttResponse{
			// We have no asynchronous SUBSCRIPTIONS, so our packets won't
			// have packet ID's. But, if we ever do them, this is one place that
			// we won't have to change our code.
			PacketID: subAck.PacketID,
		}
		for i, code := range subAck.ReturnCodes {
			if code == mqttpacket.QOSFailure {
				err := errors.New("subscription rejected")
				if i == 0 {
					// If someone just checks Err of the response, at least
					// they'll know that there was a failure
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
		if err := conn.conn.SetReadDeadline(time.Now().Add(connResponseDeadline)); err != nil {
			logError("[MQTT] failed to set read deadline: %s", err.Error())
			break
		}
		p, err := conn.stream.Read()
		if err != nil {
			// Disconnect "responses" are EOF
			if err == io.EOF || conn.getStatus() == DisconnectedNetworkStatus {
				// Server disconnected. This happens for 2 reasons:
				// 1. We initiated a disconnect
				// 2. we don't ping, so the server assumed we're done
				conn.setStatus(DisconnectedNetworkStatus)
				logInfo("[MQTT] net.Conn disconnected: %s", err.Error())
			} else {
				// I/O Errors usually return this, so if it is, we can
				// figure out what to do next
				if opError, ok := err.(*net.OpError); ok {
					if os.IsTimeout(opError.Err) {
						// No problem - read deadline just exceeded
						continue
					}
				}
				logError("[MQTT] failed to read packet: %v", err)
			}
			// The signal to the caller that disconnect was complete is the
			// exiting of this function (and wg.Done())
			break
		}
		// Wait until here to set last activity, since disconnects and timeouts
		// should be included as activity.
		conn.setLastActivity(time.Now())
		if p.Type() == mqttpacket.PUBLISH {
			// Incoming publish, received from our subscription.
			pubPkt := p.(*mqttpacket.PublishPacket)
			conn.Receiver.handlePubReceive(pubPkt, time.Now())
		} else {
			// Everything besides publish and disconnect, so unpackage the
			// packet data and send it to the appropriate channel
			respData := conn.createResponseForPacket(p)
			respCh := conn.getResponseChannel(p.Type())
			select {
			case respCh <- respData:
			default:
				logInfo("[MQTT] Queueing error as nil (p.Type: %s / respData: %+v)", p.Type(), respData)
				conn.sendQueueingError(nil)
			}
		}
	}
}

func (conn *mqttConn) writePacket(p mqttpacket.Packet) error {
	// XXX - I've used a SetWriteDeadline() for this, even on Flush, but I've
	// never gotten the writes to time out. I think it's because the underlying
	// stream is buffered. It still doesn't quite make sense, because Flush() on
	// the buffered stream still forces a Write on the unbuffered Writer. My
	// guess is that it's the nature of TCP. If there's no failure, even the
	// lack of an ACK on write won't result in timing out. But, in any case, we
	// will still have a response timeout on the round trip, which might be
	// sufficient.
	if err := conn.conn.SetWriteDeadline(time.Now().Add(connResponseDeadline)); err != nil {
		logError("[MQTT] failed to set write deadline: %s", err.Error())
		return err
	}
	if err := conn.stream.Write(p); err != nil {
		logError("[MQTT] failed to send %s packet: %s", p.Type(), err.Error())
		return err
	}

	if err := conn.conn.SetWriteDeadline(time.Now().Add(connResponseDeadline)); err != nil {
		logError("[MQTT] failed to set write deadline: %s", err.Error())
		return err
	}
	if err := conn.stream.Flush(); err != nil {
		logError("[MQTT] failed to flush %s packet: %s", p.Type(), err.Error())
		return err
	}

	// Do not call setLastActivity here.
	// Write will succeed without error even if the packet is lost somewhere
	// before reaching the server.
	// The only way to know the actual network activity is to watch the packets
	// from the server.

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

// Sanity check to verify that we are queueing or non-queueing the correct
// types of packets. ACK's are not queued, of course, but since we can use this
// function to make route returns, we handle ACK's in this function too.
func (conn *mqttConn) verifyPacketType(pktType mqttpacket.Type) packetSendType {
	switch pktType {
	case mqttpacket.CONNECT, mqttpacket.CONNACK, mqttpacket.DISCONNECT, mqttpacket.SUBSCRIBE,
		mqttpacket.SUBACK:
		return directPacketSendType
	case mqttpacket.PUBLISH, mqttpacket.PUBACK, mqttpacket.PINGREQ, mqttpacket.PINGRESP:
		return queuedPacketSendType
	default:
		logError("[MQTT] Unhandled packet type: %s", pktType)
		return unhandledPacketSendType
	}
}

func (conn *mqttConn) getResponseChannel(pktType mqttpacket.Type) chan MqttResponse {
	switch pktType {
	case mqttpacket.CONNECT, mqttpacket.CONNACK:
		return conn.connRespCh
	case mqttpacket.SUBSCRIBE, mqttpacket.SUBACK:
		return conn.subRespCh
	case mqttpacket.UNSUBSCRIBE, mqttpacket.UNSUBACK:
		// While using the same channel as subscribe probably wouldn't be a
		// problem, it's just safer to use a separate channel for unsubs.
		return conn.unsubRespCh
	case mqttpacket.PUBLISH, mqttpacket.PUBACK:
		return conn.queueAckCh
	case mqttpacket.PINGREQ, mqttpacket.PINGRESP:
		return conn.pingRespCh
	case mqttpacket.DISCONNECT:
		return nil
	default:
		logError("[MQTT] Unhandled packet type: %s", pktType)
		return nil
	}
}

func (conn *mqttConn) sendQueueingError(err error) {
	if err == nil {
		err = errors.New("channel full. sending on error channel")
	}
	if conn.errCh != nil {
		select {
		case conn.errCh <- err:
			logInfo("Error Queued to delegate %d/%d", len(conn.errCh),
				cap(conn.errCh))
		default:
			logInfo("Error delegate channel full. Check TakeRemainingErrors")
			conn.Receiver.appendError(err)
		}
	} else {
		logInfo("No Error delegate channel. Check TakeRemainingErrors")
		conn.Receiver.appendError(err)
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
