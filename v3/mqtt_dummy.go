package mode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	packet "github.com/moderepo/device-sdk-go/v2/mqtt_packet"
)

// Not really for export, but
type publishMode int

const (
	publishKvUpdate publishMode = iota
	publishCommand
	publishNone
)

var (
	testMqttHost               = "localhost"
	testMqttPort               = 1998
	useTLS                     = false
	requestTimeout             = 2 * time.Second
	_pubMode       publishMode = publishNone
	currentDevice              = ""
)

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

func readStream(conn net.Conn) bool {
	keepConn := true
	for keepConn {
		tmp := make([]byte, 256)
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				logError("[MQTTD] Error reading packet from client: %s", err)
			}
			break
		}
		l, ty := packet.DetectPacket(tmp[0:n])
		var pkt packet.Packet
		if ty == packet.CONNECT {
			inPkt := packet.NewConnectPacket()
			inPkt.Decode(tmp[0:l])
			pkt, keepConn = doConnect(inPkt)
		} else if ty == packet.SUBSCRIBE {
			pkt = &packet.SubackPacket{PacketID: 1, ReturnCodes: []byte{packet.QOSAtMostOnce, packet.QOSAtMostOnce}}
		} else if ty == packet.PINGREQ {
			pkt = packet.NewPingrespPacket()
		} else if ty == packet.DISCONNECT {
			// Let the caller close
			return false
		} else if ty == packet.PUBLISH {
			inPkt := packet.NewPublishPacket()
			inPkt.Decode(tmp[0:l])
			// If it's QOS 1, create an ack packet.
			if inPkt.Message.QOS == packet.QOSAtLeastOnce {
				pubAck := packet.NewPubackPacket()
				pubAck.PacketID = inPkt.PacketID
				pkt = pubAck
			} else {
				continue
			}
		} else {
			logInfo("[MQTTD] Unhandled data from client: %s (%s)", l, ty)
			continue
		}

		if err := writePacket(conn, pkt, false); err != nil {
			logError("[MQTTD] Error writing packet to client: %s", err)
			break
		}
	}
	return false
}

func handleSession(conn net.Conn) {
	// Read until
	if readStream(conn) {
		return
	}
	conn.Close()
}

func handlePublish(conn net.Conn) {
	var pkt packet.Packet

	switch _pubMode {
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
	case publishNone:
		// just return
	}

	if pkt != nil {
		timer := time.NewTimer(2 * time.Second)
		<-timer.C

		if err := writePacket(conn, pkt, true); err != nil {
			logError("[MQTTD] Error writing packet to client: %s", err)
		}
	}
}

func startAccepting(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleSession(conn)
		go handlePublish(conn)
	}
}

func startServing(ctx context.Context, wg *sync.WaitGroup, readyCh chan struct{}) {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", testMqttHost, testMqttPort))
	if err != nil {
		panic(err)
	}

	go startAccepting(l)
	close(readyCh)
	<-ctx.Done()
	logInfo("[MQTTD] Shutting down server")
	l.Close()
	wg.Done()
}

// Dummy server, with a context that will shut us down
// modeMode means run as a Mode MQTT Server, not a generic one
func dummyMQTTD(ctx context.Context, wg *sync.WaitGroup, mode publishMode) {
	_pubMode = mode
	readyCh := make(chan struct{})

	go startServing(ctx, wg, readyCh)
	// Wait to return until we've started the server
	<-readyCh
}
