package mode

import (
	"fmt"
	"io"
	"net"
	"sync"

	packet "github.com/moderepo/device-sdk-go/mqtt_packet"
)

type MqttTestDaemon struct {
	TestPort     int
	WG           sync.WaitGroup
	WaitOn       map[string]bool
	WriteChannel chan packet.PublishPacket
	ExitChannel  chan bool
	Results      map[string]string
}

func (m *MqttTestDaemon) Reset() {
	m.WaitOn = map[string]bool{}
	m.Results = map[string]string{}
}

func (m *MqttTestDaemon) readStream(conn net.Conn) bool {
	println("\n[MqttTestDaemon]readStream starts")
	defer func() {
		println("\n[MqttTestDaemon] readStream ends")
	}()
	for {
		tmp := make([]byte, 256)
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				fmt.Println("[MqttTestDaemon] read error:", err)
			}
			break
		}
		l, ty := packet.DetectPacket(tmp[0:n])
		var pkt packet.Packet
		var pub *packet.PublishPacket = nil
		if ty == packet.CONNECT {
			pkt = &packet.ConnackPacket{ReturnCode: packet.ConnectionAccepted}
		} else if ty == packet.SUBSCRIBE {
			pkt = &packet.SubackPacket{PacketID: 1, ReturnCodes: []byte{packet.QOSAtMostOnce, packet.QOSAtMostOnce, packet.QOSAtMostOnce}}
		} else if ty == packet.PUBLISH {
			pub = packet.NewPublishPacket()
			pub.Decode(tmp[0:n])
			pkt = &packet.PubackPacket{PacketID: pub.PacketID}
		} else {
			fmt.Println("[MqttTestDaemon] tcp buffer:", l, ty)
			break
		}

		dst := make([]byte, 100)
		n2, err := pkt.Encode(dst)
		if err != nil {
			fmt.Println("[MqttTestDaemon] encode error:", err)
			break
		}
		if _, err = conn.Write(dst[0:n2]); err != nil {
			fmt.Println("Write error:", err)
			break
		}
		if pub != nil {
			m.Results["PubMessage"] = pub.Message.String()
			m.Results["PubTopic"] = pub.Message.Topic
			if _, ok := m.WaitOn["pub"]; ok {
				m.WG.Done()
			}
			return true
		}
	}
	return false
}

func (m *MqttTestDaemon) writeStream(conn net.Conn) bool {
	println("\n[MqttTestDaemon] writeStream starts")
	defer func() {
		println("\n[MqttTestDaemon] writeStream ends")
	}()

	for {
		select {
		case pkt := <-m.WriteChannel:
			dst := make([]byte, 100)
			n, err := pkt.Encode(dst)
			if err != nil {
				fmt.Println("[MqttTestDaemon] Encode error:", err)
				return false
			}
			if _, err = conn.Write(dst[0:n]); err != nil {
				fmt.Println("[MqttTestDaemon] Write error:", err)
				return false
			}
			if _, ok := m.WaitOn["writeStream"]; ok {
				m.WG.Done()
			}
		default:
			return true
		}
	}
	return true
}

func (m *MqttTestDaemon) Start() {
	l, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", m.TestPort))
	if err != nil {
		panic(err)
	}
	defer func() {
		l.Close()
		println("[MqttTestDaemon] loop has ended")
	}()

	for {
		select {
		case e := <-m.ExitChannel:
			println("\n[MqttTestDaemon] Closing", e)
			return
		default:
			println("[MqttTestDaemon] looping")
		}
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		m.readStream(conn)
		m.writeStream(conn)
		conn.Close()
	}
	return
}

func (m *MqttTestDaemon) ShutDown() {
	println("[MqttTestDaemon] Shutdowning")
	m.ExitChannel <- true
}
