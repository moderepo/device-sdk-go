package mode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	packet "github.com/moderepo/device-sdk-go/v3/mqtt_packet"
)

type DummyCmd int

const (
	// Tells the server to publish on whatever topics a client is subscribed to
	PublishCmd DummyCmd = iota
	// Tells the server to publish some kv commands
	PublishKvSync
	PublishKvSet
	PublishKvDelete
	// Tells the server to publish a command
	PublishCommandCmd
	// Shuts the server down
	ShutdownCmd
	// Disconnects all connections
	DisconnectCmd
	// Inserts a 3 second delay after receiving the next command
	SlowdownServerCmd
	// Resets the server to normal
	ResetServerCmd
)

var (
	DefaultMqttHost = "localhost"
	DefaultMqttPort = 1998
	DefaultUsers    = []string{"good", "1234"}
	DefaultSubData  []byte

	DefaultItems = []*KeyValue{
		&KeyValue{Key: "key1", Value: "value1", ModificationTime: time.Now()},
		&KeyValue{Key: "key2", Value: "value2", ModificationTime: time.Now()},
	}

	currentDevice = "" // XXX - fix the logic that stores this globally

	globalConf DummyConfig
	packetID   uint32 = 1
	kvRevision uint32 = 1

	// DummyServerDelayDuration is the amount of time the server will wait before
	// responding. This can be used to simulate a slow network
	DummyServerDelayDuration = 3 * time.Second

	MqttDummyContext *DummyContext
)

type (
	DummyConfig struct {
		MqttHost string
		MqttPort int
		Users    []string
		SubData  []byte
	}

	// current connections
	DummyClient struct {
		conn          net.Conn
		Subscriptions []string
		subsMtx       sync.RWMutex
	}

	DummyContext struct {
		ctx         context.Context
		cmdCh       chan DummyCmd
		listener    net.Listener
		Clients     []*DummyClient
		clientsMtx  sync.RWMutex
		waitTime    time.Duration
		waitTimeMtx sync.RWMutex
	}
)

func (c *DummyClient) addSubscriptions(s string) {
	c.subsMtx.Lock()
	defer c.subsMtx.Unlock()
	c.Subscriptions = append(c.Subscriptions, s)
}

func (c *DummyContext) addClient(client *DummyClient) {
	c.clientsMtx.Lock()
	defer c.clientsMtx.Unlock()
	c.Clients = append(c.Clients, client)
}

func (c *DummyContext) removeClient(client *DummyClient) {
	c.clientsMtx.Lock()
	defer c.clientsMtx.Unlock()
	for i, clnt := range c.Clients {
		if clnt == client {
			c.Clients[i] = nil
		}
	}
}

func (c *DummyContext) setWaitTime(t time.Duration) {
	c.waitTimeMtx.Lock()
	defer c.waitTimeMtx.Unlock()
	c.waitTime = t
}

func (c *DummyContext) getWaitTime() time.Duration {
	c.waitTimeMtx.RLock()
	defer c.waitTimeMtx.RUnlock()
	return c.waitTime
}

func isValidUser(user string, testUsers []string) bool {
	for _, vUser := range testUsers {
		if user == vUser {
			return true
		}
	}

	return false
}

func doConnect(connPkt *packet.ConnectPacket) (*packet.ConnackPacket, bool) {
	ackPack := packet.NewConnackPacket()
	keepConn := false
	if isValidUser(connPkt.Username, globalConf.Users) {
		logInfo("[MQTTD] accepting username: %s", connPkt.Username)
		currentDevice = connPkt.Username
		ackPack.ReturnCode = packet.ConnectionAccepted
		keepConn = true
	} else {
		logInfo("[MQTTD] rejecting username: %s", connPkt.Username)
		ackPack.ReturnCode = packet.ErrBadUsernameOrPassword
	}
	return ackPack, keepConn
}

func writePacket(conn net.Conn, pkt packet.Packet, bigPacket bool) error {
	pktBufSize := 100
	if bigPacket {
		pktBufSize = 300
	}
	dst := make([]byte, pktBufSize)
	n2, err := pkt.Encode(dst)
	if err != nil {
		return err
	}
	if _, err = conn.Write(dst[0:n2]); err != nil {
		return err
	}
	return nil
}

func readPacket(conn net.Conn) (bytesRead []byte, err error) {
	tmp := make([]byte, 256)
	numBytes, err := conn.Read(tmp)
	if err != nil {
		if err == io.EOF {
			// EOF means client closed the connection. We treat this as a
			// normal operation.
			return tmp[0:numBytes], nil
		}
		logError("[MQTTD] Error reading packet from client: %s", err)
		return []byte{}, err
	}
	return tmp[0:numBytes], nil
}

func handleSession(context *DummyContext, client *DummyClient) {
	var remain []byte

	for {
		// Read until command says quit or we get a disconnect
		packetBytes, err := readPacket(client.conn)
		if context.getWaitTime() > 0 {
			logInfo("[MQTTD] Pausing server...")
			timer := time.NewTimer(context.getWaitTime())
			<-timer.C
			logInfo("[MQTTD] Unpausing server...")
		}

		if err != nil {
			logError("[MQTTD] Error reading packet from stream: %s", err)
			// we can't continue without a packet from the client
			break
		}

		packetBytes = append(remain, packetBytes...)
		if len(packetBytes) <= 0 {
			continue
		}

		var respPkts []packet.Packet
		var keepConn bool
		respPkts, remain, keepConn = getResponsePacket(client, packetBytes)

		for _, respPkt := range respPkts {
			logInfo("[MQTTD] writePacket on handleSession %+v", respPkt)
			if respPkt == nil {
				continue
			}
			if err := writePacket(client.conn, respPkt, false); err != nil {
				logError("[MQTTD] Error writing packet to client on handleSession: %s", err)
				// write error is likely a missing client, so we end the
				// connection
				keepConn = false
			}
		}
		if !keepConn {
			break
		}
	}
	logInfo("[MQTTD] closing connection")
	client.conn.Close()
}
func parsePacket(client *DummyClient, ty packet.Type, pktBytes []byte) (pkt packet.Packet, keepConn bool) {
	keepConn = true
	if ty == packet.CONNECT {
		inPkt := packet.NewConnectPacket()
		inPkt.Decode(pktBytes)
		pkt, keepConn = doConnect(inPkt)
	} else if ty == packet.SUBSCRIBE {
		inPkt := packet.NewSubscribePacket()
		inPkt.Decode(pktBytes)
		for _, sub := range inPkt.Subscriptions {
			client.addSubscriptions(sub.Topic)
		}
		// Add the topics to the client
		pkt = &packet.SubackPacket{
			PacketID:    uint16(atomic.AddUint32(&packetID, 1)),
			ReturnCodes: []byte{packet.QOSAtMostOnce, packet.QOSAtMostOnce}}
	} else if ty == packet.UNSUBSCRIBE {
		inPkt := packet.NewUnsubscribePacket()
		inPkt.Decode(pktBytes)
		// Remove the subscriptions from the current subs. If we don't find it,
		// there's no return code for this packet, so no error
		for _, sub := range inPkt.Topics {
			// It's a bit of work to remove the sub, so just make it empty. When
			// we iterate later, we'll have to make sure it's not empty before
			// we try to use it.
			for index, currentSub := range client.Subscriptions {
				if currentSub == sub {
					client.Subscriptions[index] = ""
				}
			}
		}
		pkt = &packet.UnsubackPacket{
			PacketID: uint16(atomic.AddUint32(&packetID, 1))}
	} else if ty == packet.PINGREQ {
		pkt = packet.NewPingrespPacket()
	} else if ty == packet.DISCONNECT {
		pkt = nil
		keepConn = false
	} else if ty == packet.PUBLISH {
		inPkt := packet.NewPublishPacket()
		inPkt.Decode(pktBytes)
		// If it's QOS 1, create an ack packet.
		if inPkt.Message.QOS == packet.QOSAtLeastOnce {
			pubAck := packet.NewPubackPacket()
			pubAck.PacketID = inPkt.PacketID
			pkt = pubAck
		} else {
			pkt = nil
		}
	}

	return pkt, keepConn

}

func getResponsePacket(client *DummyClient, pktBytes []byte) ([]packet.Packet, []byte, bool) {
	keepConn := true
	var packets []packet.Packet
	tmp := pktBytes
	for {
		l, ty := packet.DetectPacket(tmp)
		if len(tmp) < l {
			return packets, tmp, true
		}
		pkt, ok := parsePacket(client, ty, tmp[0:l])
		keepConn = ok

		packets = append(packets, pkt)
		if len(tmp) <= l {
			break
		}
		tmp = tmp[l:]
	}
	return packets, []byte{}, keepConn
}

func packageKVPacket(kvData *KeyValueSync, topic string) packet.Packet {
	data, _ := json.Marshal(kvData)
	m := packet.Message{
		Topic:   topic,
		Payload: data,
		QOS:     packet.QOSAtMostOnce,
	}
	kvPkt := packet.NewPublishPacket()
	kvPkt.Message = m
	kvPkt.PacketID = uint16(atomic.AddUint32(&packetID, 1))

	return kvPkt
}

func sendPublish(client *DummyClient, pubCmd DummyCmd) {
	if client == nil {
		return
	}
	var pkts []packet.Packet

	switch pubCmd {
	case PublishCmd:
		client.subsMtx.RLock() // to guard client.subscriptions
		for _, topic := range client.Subscriptions {
			if topic == "" {
				continue
			}
			// publish some data on the clients' topics. (If we want specific
			// data, we'll have to use some new mechanism.)
			pubPkt := packet.NewPublishPacket()
			pubPkt.Message = packet.Message{
				Topic:   topic,
				Payload: globalConf.SubData,
				QOS:     packet.QOSAtMostOnce,
			}
			pubPkt.PacketID = uint16(atomic.AddUint32(&packetID, 1))
			pkts = append(pkts, pubPkt)
		}
		client.subsMtx.RUnlock()
	case PublishKvSync:
		kvData := KeyValueSync{
			Action:   KVSyncActionReload,
			Revision: int(atomic.AddUint32(&kvRevision, uint32(1))),
			NumItems: 2,
			Items:    DefaultItems,
		}
		pkt := packageKVPacket(&kvData,
			fmt.Sprintf("/devices/%s/kv", currentDevice))
		pkts = append(pkts, pkt)
	case PublishKvSet:
		kvData := KeyValueSync{
			Action:   KVSyncActionSet,
			Revision: int(atomic.AddUint32(&kvRevision, uint32(1))),
			Key:      "key3",
			Value:    "value3",
		}
		pkt := packageKVPacket(&kvData,
			fmt.Sprintf("/devices/%s/kv", currentDevice))
		pkts = append(pkts, pkt)
	case PublishKvDelete:
		kvData := KeyValueSync{
			Action:   KVSyncActionDelete,
			Revision: int(atomic.AddUint32(&kvRevision, uint32(1))),
			Key:      "key3",
		}
		pkt := packageKVPacket(&kvData,
			fmt.Sprintf("/devices/%s/kv", currentDevice))
		pkts = append(pkts, pkt)
	case PublishCommandCmd:
		var cmd struct {
			Action     string            `json:"action" binding:"required"`
			Parameters map[string]string `json:"parameters,omitempty"`
		}
		cmd.Action = "doEcho"
		cmd.Parameters = map[string]string{"Key1": "Value"}
		cmdData, _ := json.Marshal(cmd)
		m := packet.Message{
			Topic:   fmt.Sprintf("/devices/%s/command", currentDevice),
			Payload: cmdData,
			QOS:     packet.QOSAtMostOnce,
		}
		cmdPkt := packet.NewPublishPacket()
		cmdPkt.Message = m
		cmdPkt.PacketID = 2
		pkts = append(pkts, cmdPkt)
	}

	for _, pkt := range pkts {
		timer := time.NewTimer(1 * time.Second)
		<-timer.C

		if err := writePacket(client.conn, pkt, true); err != nil {
			logError("[MQTTD] Error writing packet to client on sendPublish: %s", err)
		}
	}
}

func spawnSession(context *DummyContext, conn net.Conn) {
	client := &DummyClient{
		conn: conn,
	}
	context.addClient(client)
	defer context.removeClient(client)

	handleSession(context, client)
}

func runServer(wg *sync.WaitGroup, context *DummyContext, listener net.Listener) {
	defer wg.Done()
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		logInfo("[MQTTD] Starting new session")
		go spawnSession(context, conn)
	}
}

func runCommandHandler(wg *sync.WaitGroup, context *DummyContext) {
	defer wg.Done()
	for cmd := range context.cmdCh {
		switch cmd {
		case PublishCmd, PublishKvSync, PublishKvSet, PublishKvDelete,
			PublishCommandCmd:
			context.clientsMtx.RLock() // to guard context.clients
			for _, client := range context.Clients {
				go sendPublish(client, cmd)
			}
			context.clientsMtx.RUnlock()
		case SlowdownServerCmd:
			context.setWaitTime(DummyServerDelayDuration)
		case DisconnectCmd:
			closeConnections(context)
		case ResetServerCmd:
			context.setWaitTime(0)
		case ShutdownCmd:
			performShutdown(context)
		}
	}
}

func runWaitForCancel(wg *sync.WaitGroup, context *DummyContext) {
	defer wg.Done()
	<-context.ctx.Done()
	performShutdown(context)
}

func performShutdown(context *DummyContext) {
	// Tells the server to not accept new connections
	context.listener.Close()
	closeConnections(context)
	if context.cmdCh != nil {
		// close the command channel so it doesn't listen to any more commands
		close(context.cmdCh)
	}
}

func closeConnections(context *DummyContext) {
	context.clientsMtx.Lock()
	defer context.clientsMtx.Unlock()

	for _, client := range context.Clients {
		if client != nil {
			client.conn.Close()
		}
	}
	context.Clients = nil
}

// Dummy MQTT server. It will run an MQTT server as a goroutine. An optional
// command channel can be passed in to manipulate the server manipulating test
// conditions and shutting down.
// Unlike the v2 dummyMQTTD, this starts goroutines but isn't meant to be run as
// a goroutine. It will panic if it is unable to start listening.
//
// To end the server, either close the command channel or call cancel() on the
// context.
func DummyMQTTD(ctx context.Context, wg *sync.WaitGroup,
	cmdCh chan DummyCmd) bool {

	conf := DummyConfig{
		MqttHost: DefaultMqttHost,
		MqttPort: DefaultMqttPort,
		Users:    DefaultUsers,
	}
	return DummyMQTTDWithConfig(ctx, wg, cmdCh, conf)
}

func DummyMQTTDWithConfig(ctx context.Context, wg *sync.WaitGroup,
	cmdCh chan DummyCmd, conf DummyConfig) bool {
	globalConf = conf
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.MqttHost,
		conf.MqttPort))
	if err != nil {
		panic(err)
	}

	context := DummyContext{
		ctx:      ctx,
		cmdCh:    cmdCh,
		listener: l,
		waitTime: 0,
	}

	if cmdCh != nil && ctx != nil {
		panic(errors.New("dummy server cannot run with context.Context *and* a command channel"))
	}
	if cmdCh != nil {
		go runCommandHandler(wg, &context)
	} else {
		// No command channel, so wait for the context to shut us down
		go runWaitForCancel(wg, &context)
	}
	wg.Add(1)
	go runServer(wg, &context, l)
	MqttDummyContext = &context

	return true
}
