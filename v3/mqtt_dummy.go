package mode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	packet "github.com/moderepo/device-sdk-go/v3/mqtt_packet"
)

type publishMode int
type dummyCmd int

const (
	publishKvUpdate publishMode = iota
	publishCommand

	// publishes a kv sync
	publishKvCmd dummyCmd = iota
	// publishes a command
	publishCommandCmd
	shutdownCmd
	slowdownServerCmd
	resetServerCmd
)

var (
	testMqttHost   = "localhost"
	testMqttPort   = 1998
	useTLS         = false
	requestTimeout = 2 * time.Second
	currentDevice  = ""
)

type dummyContext struct {
	ctx      context.Context
	cmdCh    chan dummyCmd
	listener net.Listener
	conns    []net.Conn
	waitTime time.Duration
}

func doConnect(connPkt *packet.ConnectPacket) (*packet.ConnackPacket, bool) {
	ackPack := packet.NewConnackPacket()
	keepConn := false
	if connPkt.Username == "good" || connPkt.Username == "1234" {
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

func handleSession(context *dummyContext, conn net.Conn) {
	keepConn := true

	for keepConn {
		// Read until command says quit or we get a disconnect
		bytesRead, packetBytes, err := readPacket(conn)
		if context.waitTime > 0 {
			logInfo("[MQTTD] Pausing server...")
			timer := time.NewTimer(context.waitTime)
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
		respPkt, keepConn = getResponsePacket(packetBytes, bytesRead)
		if respPkt != nil {
			if err := writePacket(conn, respPkt, false); err != nil {
				logError("[MQTTD] Error writing packet to client: %s", err)
				// write error is likely a missing client, so we end the connection
				keepConn = false
			}
		}
	}
	logInfo("[MQTTD] closing connection")
	conn.Close()
}

func getResponsePacket(pktBytes []byte, pktLen int) (pkt packet.Packet, keepConn bool) {
	keepConn = true
	l, ty := packet.DetectPacket(pktBytes[0:pktLen])
	logInfo("[MQTTD] Server Received %d", ty)
	if ty == packet.CONNECT {
		inPkt := packet.NewConnectPacket()
		inPkt.Decode(pktBytes[0:l])
		pkt, keepConn = doConnect(inPkt)
	} else if ty == packet.SUBSCRIBE {
		pkt = &packet.SubackPacket{PacketID: 1, ReturnCodes: []byte{packet.QOSAtMostOnce, packet.QOSAtMostOnce}}
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

func sendPublish(conn net.Conn, pubMode publishMode) {
	var pkt packet.Packet

	switch pubMode {
	case publishKvUpdate:
		kvData := KeyValueSync{
			Action:   KVSyncActionReload,
			Revision: 2,
			NumItems: 2,
			Items: []*KeyValue{
				&KeyValue{Key: "key1", Value: "value1", ModificationTime: time.Now()},
				&KeyValue{Key: "key2", Value: "value2", ModificationTime: time.Now()},
			},
		}
		data, _ := json.Marshal(kvData)
		m := packet.Message{
			Topic:   fmt.Sprintf("/devices/%s/kv", currentDevice),
			Payload: data,
			QOS:     packet.QOSAtMostOnce,
		}
		kvPkt := packet.NewPublishPacket()
		kvPkt.Message = m
		kvPkt.PacketID = 1
		pkt = kvPkt
	case publishCommand:
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
		pkt = cmdPkt
	}

	if pkt != nil {
		timer := time.NewTimer(1 * time.Second)
		<-timer.C

		if err := writePacket(conn, pkt, true); err != nil {
			logError("[MQTTD] Error writing packet to client: %s", err)
		}
	}
}

func runServer(wg *sync.WaitGroup, context *dummyContext, listener net.Listener) {
	defer wg.Done()
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		context.conns = append(context.conns, conn)
		go handleSession(context, conn)
	}
}

func runCommandHandler(wg *sync.WaitGroup, context *dummyContext) {
	defer wg.Done()
	for cmd := range context.cmdCh {
		switch cmd {
		case publishKvCmd:
			for _, conn := range context.conns {
				go sendPublish(conn, publishKvUpdate)
			}
		case publishCommandCmd:
			for _, conn := range context.conns {
				go sendPublish(conn, publishCommand)
			}
		case slowdownServerCmd:
			context.waitTime = 3 * time.Second
		case resetServerCmd:
			context.waitTime = 0
		case shutdownCmd:
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
	// Close the current connection
	for _, conn := range context.conns {
		conn.Close()
	}
	if context.cmdCh != nil {
		// close the command channel so it doesn't listen to any more commands
		close(context.cmdCh)
	}
}

// Dummy MQTT server. It will run an MQTT server as a goroutine. An optional
// command channel can be passed in to manipulate the server manipulating test
// conditions and shutting down.
// Unlike the v2 dummyMQTTD, this starts goroutines but isn't meant to be run as
// a goroutine. It will panic if it is unable to start listening.
//
// To end the server, either close the command channel or call cancel() on the
// context.
func dummyMQTTD(ctx context.Context, wg *sync.WaitGroup,
	cmdCh chan dummyCmd) bool {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", testMqttHost, testMqttPort))
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
