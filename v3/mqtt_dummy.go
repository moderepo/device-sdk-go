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
)

type (
	DummyConfig struct {
		MqttHost string
		MqttPort int
		Users    []string
		SubData  []byte
	}

	// current connections
	dummyClient struct {
		conn          net.Conn
		subscriptions []string
		mutex         sync.Mutex
	}

	dummyContext struct {
		ctx      context.Context
		cmdCh    chan DummyCmd
		listener net.Listener
		clients  []*dummyClient
		waitTime time.Duration
		mutex    sync.Mutex
	}
)

func (c *dummyClient) addSubscriptions(s string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.subscriptions = append(c.subscriptions, s)
}

func (c *dummyContext) addClient(client *dummyClient) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.clients = append(c.clients, client)
}

func (c *dummyContext) removeClient(client *dummyClient) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, clnt := range c.clients {
		if clnt == client {
			c.clients[i] = nil
		}
	}
}

func (c *dummyContext) setWaitTime(t time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.waitTime = t
}

func (c *dummyContext) getWaitTime() time.Duration {
	c.mutex.Lock()
	defer c.mutex.Unlock()
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

func readPacket(conn net.Conn) (numBytes int, bytesRead []byte, err error) {
	tmp := make([]byte, 256)
	numBytes, err = conn.Read(tmp)
	if err != nil {
		if err == io.EOF {
			// EOF means client closed the connection. We treat this as a
			// normal operation.
			return numBytes, tmp[0:1], nil
		}
		logError("[MQTTD] Error reading packet from client: %s", err)
		return 0, nil, err
	}
	return numBytes, tmp, nil
}

func handleSession(context *dummyContext, client *dummyClient) {
	keepConn := true

	for keepConn {
		// Read until command says quit or we get a disconnect
		bytesRead, packetBytes, err := readPacket(client.conn)
		if context.getWaitTime() > 0 {
			logInfo("[MQTTD] Pausing server...")
			timer := time.NewTimer(context.getWaitTime())
			<-timer.C
			logInfo("[MQTTD] Unpausing server...")
		}

		if err != nil {
			logError("[MQTTD] Error reading packet from stream: %s", err)
			keepConn = false
			// we can't continue without a packet from the client
			break
		}
		var respPkt packet.Packet
		respPkt, keepConn = getResponsePacket(client, packetBytes, bytesRead)

		if respPkt != nil {
			if err := writePacket(client.conn, respPkt, false); err != nil {
				logError("[MQTTD] Error writing packet to client: %s", err)
				// write error is likely a missing client, so we end the
				// connection
				keepConn = false
			}
		}
	}
	logInfo("[MQTTD] closing connection")
	client.conn.Close()
}

func getResponsePacket(client *dummyClient, pktBytes []byte,
	pktLen int) (pkt packet.Packet, keepConn bool) {
	keepConn = true
	l, ty := packet.DetectPacket(pktBytes[0:pktLen])
	if ty == packet.CONNECT {
		inPkt := packet.NewConnectPacket()
		inPkt.Decode(pktBytes[0:l])
		pkt, keepConn = doConnect(inPkt)
	} else if ty == packet.SUBSCRIBE {
		inPkt := packet.NewSubscribePacket()
		inPkt.Decode(pktBytes[0:l])
		for _, sub := range inPkt.Subscriptions {
			client.addSubscriptions(sub.Topic)
		}
		// Add the topics to the client
		pkt = &packet.SubackPacket{
			PacketID:    uint16(atomic.AddUint32(&packetID, 1)),
			ReturnCodes: []byte{packet.QOSAtMostOnce, packet.QOSAtMostOnce}}
	} else if ty == packet.UNSUBSCRIBE {
		inPkt := packet.NewUnsubscribePacket()
		inPkt.Decode(pktBytes[0:l])
		// Remove the subscriptions from the current subs. If we don't find it,
		// there's no return code for this packet, so no error
		for _, sub := range inPkt.Topics {
			// It's a bit of work to remove the sub, so just make it empty. When
			// we iterate later, we'll have to make sure it's not empty before
			// we try to use it.
			for index, currentSub := range client.subscriptions {
				if currentSub == sub {
					client.subscriptions[index] = ""
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
		inPkt.Decode(pktBytes[0:l])
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

func sendPublish(client *dummyClient, pubCmd DummyCmd) {
	if client == nil {
		return
	}
	var pkts []packet.Packet

	switch pubCmd {
	case PublishCmd:
		client.mutex.Lock() // to guard client.subscriptions
		for _, topic := range client.subscriptions {
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
		client.mutex.Unlock()
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
			logError("[MQTTD] Error writing packet to client: %s", err)
		}
	}
}

func spawnSession(context *dummyContext, conn net.Conn) {
	client := &dummyClient{
		conn: conn,
	}
	context.addClient(client)
	defer context.removeClient(client)

	handleSession(context, client)
}

func runServer(wg *sync.WaitGroup, context *dummyContext, listener net.Listener) {
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

func runCommandHandler(wg *sync.WaitGroup, context *dummyContext) {
	defer wg.Done()
	for cmd := range context.cmdCh {
		switch cmd {
		case PublishCmd, PublishKvSync, PublishKvSet, PublishKvDelete,
			PublishCommandCmd:
			context.mutex.Lock() // to guard context.clients
			for _, client := range context.clients {
				go sendPublish(client, cmd)
			}
			context.mutex.Unlock()
		case SlowdownServerCmd:
			context.setWaitTime(3 * time.Second)
		case DisconnectCmd:
			closeConnections(context)
		case ResetServerCmd:
			context.setWaitTime(0)
		case ShutdownCmd:
			performShutdown(context)
		}
	}
}

func runWaitForCancel(wg *sync.WaitGroup, context *dummyContext) {
	defer wg.Done()
	<-context.ctx.Done()
	performShutdown(context)
}

func performShutdown(context *dummyContext) {
	// Tells the server to not accept new connections
	context.listener.Close()
	closeConnections(context)
	if context.cmdCh != nil {
		// close the command channel so it doesn't listen to any more commands
		close(context.cmdCh)
	}
}

func closeConnections(context *dummyContext) {
	context.mutex.Lock()
	defer context.mutex.Unlock()

	for _, client := range context.clients {
		if client != nil {
			client.conn.Close()
		}
	}
	context.clients = nil
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

	context := dummyContext{
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

	return true
}
