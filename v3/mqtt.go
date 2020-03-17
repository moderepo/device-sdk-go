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
	mqttConnectTimeout   = time.Second * 10
	connResponseDeadline = time.Millisecond * 500
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
	// information to start a connection to the MqttServer
	MqttAuthDelegate interface {
		// Returns the tls usage and configuration. If useTLS is false, a nil
		// tlsConfig should be returned.
		TLSUsageAndConfiguration() (useTLS bool, tlsConfig *tls.Config)
		// Returns authentication information
		AuthInfo() (username string, password string)
	}

	// MqttReceiverDelegate methods allow the MqttClient to communicate
	// information and events back to the user.
	MqttReceiverDelegate interface {
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
		// Note: These channels will be closed when the connection is closed (from
		// a Disconnect), so the user should stop listening to these channels when
		// OnClose() is called..
		SetReceiveChannels(subRecvCh <-chan MqttSubData,
			queueAckCh <-chan MqttResponse,
			pingAckCh <-chan MqttResponse)

		// Hook so we can clean up on closing of connections
		OnClose()
	}

	// MqttConfigDelegate methods allow the MqttClient to configure itself
	// according to the requirements of the user
	MqttConfigDelegate interface {
		// Buffer size of the incoming queues to the delegate. This is the
		// size of the three receive channels
		GetReceiveQueueSize() uint16

		// Buffer size of the outgoing queue to the server. This cannot be
		// changed after a connection is created
		GetSendQueueSize() uint16
	}

	// MqttErrorDelegate is an optional delegate which allows the MqttClient
	// a method of signaling errors that are not able to be communicated
	// through the normal channels. See handling errors in the documentation.
	MqttErrorDelegate interface {
		// The buffer size of the error channel
		GetErrorChannelSize() uint16

		// Provides the delegate the channel to receive errors
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
	}

	// Delegate for the mqqtConn to call back to the MqttClient
	mqttReceiver interface {
		// Called by the connection when receiving publishes
		handlePubReceive(pkt *packet.PublishPacket, receiveTime time.Time)
		// Called by the connection when there is an error
		appendError(error)
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
		connRespCh  chan MqttResponse
		subRespCh   chan MqttResponse
		unsubRespCh chan MqttResponse
		pingRespCh  chan MqttResponse
		queueAckCh  chan MqttResponse
		// This is optional and may be nil
		errCh chan error

		mutex       sync.Mutex
		statusMutex sync.RWMutex
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
//
func (client *MqttClient) IsConnected() bool {
	return client.conn != nil && client.conn.getStatus() == ConnectedNetworkStatus
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
		return errors.New("Cannot connect when already connected")
	}
	if err := client.createMqttConnection(); err != nil {
		return err
	}

	user, pwd := client.authDelegate.AuthInfo()
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
		client.conn.setStatus(DisconnectedNetworkStatus)
		return resp.Err
	}
	// If we made it here, we consider ourselves connected
	client.conn.setStatus(ConnectedNetworkStatus)

	return nil
}

// Disconnect will end this connection with the server. We will block until
// the server closes the connection.
// Note: We might want to wait, but order is important here.
func (client *MqttClient) Disconnect(ctx context.Context) error {
	if client.conn == nil {
		// nothing to disconnect. Return
		return nil
	}

	defer func() {
		client.shutdownConnection()
	}()

	// Maybe we want to add a Connecting/Disconnecting status?
	client.conn.setStatus(DefaultNetworkStatus)
	p := packet.NewDisconnectPacket()
	_, err := client.conn.sendPacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	return nil
}

// Subscribe will query the delegate for its subscriptions via the
// Subscriptions method. This is a synchronous call so it will block until
// a response is received from the server. It will return a slice of errors
// which will be in the same order as the subscriptions in Subscriptions().
func (client *MqttClient) Subscribe(ctx context.Context,
	subs []string) []error {
	p := packet.NewSubscribePacket()
	p.Subscriptions = make([]packet.Subscription, 0, 10)
	p.PacketID = client.conn.getPacketID()

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

func (client *MqttClient) Unubscribe(ctx context.Context,
	subs []string) []error {
	p := packet.NewUnsubscribePacket()
	p.Topics = subs
	p.PacketID = client.conn.getPacketID()

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
// for delivery. Results will be sent on the delegate's pingAckCh
func (client *MqttClient) Ping(ctx context.Context) error {

	p := packet.NewPingreqPacket()
	_, err := client.conn.queuePacket(ctx, p)
	if err != nil {
		logError("[MQTT] failed to send packet: %s", err.Error())
		return err
	}

	return nil
}

// PingAndWait sends an MQTT PINGREQ event to the server and waits for the
// response. If this method is used instead of the asynchronous Ping, user
// should not be listening on the pingAckCh channel since this function may
// timeout waiting for the response an error will be returned.
func (client *MqttClient) PingAndWait(ctx context.Context) error {

	p := packet.NewPingreqPacket()
	respChan, err := client.conn.sendPacket(ctx, p)
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
		pktQos = packet.QOSAtMostOnce
	case QOSAtLeastOnce:
		pktQos = packet.QOSAtLeastOnce
	default:
		return 0, errors.New("unsupported qos level")
	}

	p := packet.NewPublishPacket()
	p.PacketID = packetID
	p.Message = packet.Message{
		Topic:   topic,
		QOS:     pktQos,
		Payload: data,
	}
	logInfo("[MQTT] Publishing on topic [%s]", p.Message.Topic)

	return client.conn.queuePacket(ctx, p)
}

// Each connect, we need to create a new mqttConnection.
func (client *MqttClient) createMqttConnection() error {
	receiveQueueSize := client.confDelegate.GetReceiveQueueSize()
	subRecvCh := make(chan MqttSubData, receiveQueueSize)
	queueAckCh := make(chan MqttResponse, receiveQueueSize)
	pingAckCh := make(chan MqttResponse, receiveQueueSize)
	client.recvDelegate.SetReceiveChannels(subRecvCh, queueAckCh, pingAckCh)
	client.delegateSubRecvCh = subRecvCh
	useTLS, tlsConfig := client.authDelegate.TLSUsageAndConfiguration()
	sendQueueSize := client.confDelegate.GetSendQueueSize()
	conn := newMqttConn(tlsConfig, client.mqttHost, client.mqttPort, useTLS,
		queueAckCh, pingAckCh, sendQueueSize)

	if conn == nil {
		return errors.New("Unable to create a socket to the server")
	}

	if client.errorDelegate != nil {
		errCh := make(chan error, client.errorDelegate.GetErrorChannelSize())
		client.errorDelegate.SetErrorChannel(errCh)
		conn.errCh = errCh
	}

	client.conn = conn
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
	// We skip encapsulation of the connection class so the steps are clear
	// 1. Send close to the packetWriter goroutine
	client.stopWriterCh <- struct{}{}
	// 2. Wait until the done channel has been read, since there are some queued
	// writes that might be sent before the done channel has bene read.
	client.wgSend.Wait()
	// 3. Close the channels writer channels
	close(client.conn.directSendPktCh)
	close(client.conn.queuedSendPktCh)
	// 4. Close the connection with the server. This will break the
	//    packet reader out of its loop. Set to disconnected here so their
	//    reader knows that it was not an error
	client.conn.setStatus(DisconnectedNetworkStatus)
	client.conn.conn.Close()
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
func (client *MqttClient) handlePubReceive(p *packet.PublishPacket,
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
		client.conn.sendQueueingError(nil)
		return
	}
	if p.Message.QOS != packet.QOSAtMostOnce {
		ackPkt := packet.NewPubackPacket()
		ackPkt.PacketID = p.PacketID
		// For everything except QOS2: if we have queued the message to the
		// user, we consider that successfully published to us, so we attempt
		// to send an ACK back to the server.
		ctx, cancel := context.WithTimeout(context.Background(),
			connResponseDeadline)
		defer cancel()
		if _, err := client.conn.queuePacket(ctx, ackPkt); err != nil {
			client.conn.sendQueueingError(err)
		}
	}
}

// Returns the last errors, and then resets the errors. If there is no
// error delegate or the error delegate's error channel is full, we "queue"
// errors in a slice that can be fetched.
func (client *MqttClient) TakeRemainingErrors() []error {
	defer func() {
		client.lastErrors = make([]error, 0, 5)
	}()
	return client.lastErrors
}

func (client *MqttClient) appendError(err error) {
	client.lastErrors = append(client.lastErrors, err)
}

func newMqttConn(tlsConfig *tls.Config, mqttHost string,
	mqttPort int, useTLS bool, queueAckCh chan MqttResponse,
	pingAckCh chan MqttResponse, outgoingQueueSize uint16) *mqttConn {

	addr := fmt.Sprintf("%s:%d", mqttHost, mqttPort)
	var conn net.Conn
	var err error
	if useTLS {
		if conn, err = tls.DialWithDialer(mqttDialer, "tcp", addr,
			tlsConfig); err != nil {
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
		connRespCh:  make(chan MqttResponse),
		subRespCh:   make(chan MqttResponse),
		unsubRespCh: make(chan MqttResponse),

		// These are passed to us by the client, with a buffer sized specified
		// by the delegate, so it is the delegate's responsibility to set the
		// size appropriately or we will start losing responses.
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

// We only queue pings (PINGREQ) and publishes (PUBLISH). Theoretically, we
// could queue subscribes (SUBSCRIBE) since they have packet ID's like
// publishes. But, for simplicity, those are synchronous, since, in practice,
// those are either on startup, or, at least, on rare occasions.
func (conn *mqttConn) queuePacket(ctx context.Context,
	p packet.Packet) (uint16, error) {
	if conn.getStatus() == DisconnectedNetworkStatus ||
		conn.getStatus() == TimingOutNetworkStatus {
		return 0, errors.New("Connection unstable. Unable to send")
	}

	packetID := uint16(0)
	if p.Type() == packet.PUBLISH {
		pubPkt := p.(*packet.PublishPacket)
		if pubPkt.PacketID != 0 {
			packetID = pubPkt.PacketID
		} else {
			packetID = conn.getPacketID()
			pubPkt.PacketID = uint16(packetID)
		}
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
	if conn.getStatus() == DisconnectedNetworkStatus ||
		conn.getStatus() == TimingOutNetworkStatus {
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
			if pktSendData.pkt.Type() == packet.PUBLISH {
				pubPkt := pktSendData.pkt.(*packet.PublishPacket)
				pktID = pubPkt.PacketID
			}
			// Get the packet ID, if any, for errors. This is a long lived channel, so
			// it's possible to be full.
			resultCh := pktSendData.resultCh
			err := conn.writePacket(pktSendData.pkt)
			if err != nil {
				// If there was an error sending, we can notify the caller
				// immediately.
				select {
				case resultCh <- MqttResponse{
					PacketID: pktID,
					Err:      err,
				}:
				default:
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
		conn.conn.SetReadDeadline(time.Now().Add(connResponseDeadline))
		p, err := conn.stream.Read()
		if err != nil {
			// Disconnect "responses" are EOF
			if err == io.EOF || conn.status == DisconnectedNetworkStatus {
				// Server disconnected. This happens for 2 reasons:
				// 1. We initiated a disconnect
				// 2. we don't ping, so the server assumed we're done
				conn.setStatus(DisconnectedNetworkStatus)
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
		// Wait until here to set last activity, since disconnects and timeouts
		// should be included as activity.
		conn.lastActivity = time.Now()
		if p.Type() == packet.PUBLISH {
			// Incoming publish, received from our subscription.
			pubPkt := p.(*packet.PublishPacket)
			conn.Receiver.handlePubReceive(pubPkt, time.Now())
		} else {
			// Everything besides publish and disconnect, so unpackage the
			// packet data and send it to the appropriate channel
			respData := conn.createResponseForPacket(p)
			respCh := conn.getResponseChannel(p.Type())
			select {
			case respCh <- respData:
			default:
				conn.sendQueueingError(nil)
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
	conn.conn.SetWriteDeadline(time.Now().Add(connResponseDeadline))
	if err := conn.stream.Write(p); err != nil {
		logError("[MQTT] failed to send %s packet: %s", p.Type(), err.Error())
		return err
	}

	conn.conn.SetWriteDeadline(time.Now().Add(connResponseDeadline))
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
	case packet.UNSUBSCRIBE, packet.UNSUBACK:
		// While using the same channel as subscribe probably wouldn't be a
		// problem, it's just safer to use a separate channel for unsubs.
		return conn.unsubRespCh
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

func (conn *mqttConn) sendQueueingError(err error) {
	if err == nil {
		err = errors.New("Channel full. Sending on error channel")
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
