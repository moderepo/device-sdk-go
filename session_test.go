package mode

import (
	"fmt"
	"io"

	//	"io/ioutil"
	"net"
	"os"
	"sync"
	"testing"

	packet "github.com/moderepo/device-sdk-go/mqtt_packet"
	"github.com/stretchr/testify/assert"
)

var testPort int = 9999
var wg sync.WaitGroup
var resultingPubMessage string
var resultingPubTopic string

func readStream(conn net.Conn) bool {
	for {
		tmp := make([]byte, 256)
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				fmt.Println("read error:", err)
			}
			break
		}
		l, ty := packet.DetectPacket(tmp[0:n])
		var pkt packet.Packet
		var pub *packet.PublishPacket = nil
		if ty == packet.CONNECT {
			pkt = &packet.ConnackPacket{ReturnCode: packet.ConnectionAccepted}
		} else if ty == packet.SUBSCRIBE {
			pkt = &packet.SubackPacket{PacketID: 1, ReturnCodes: []byte{packet.QOSAtMostOnce, packet.QOSAtMostOnce}}
		} else if ty == packet.PUBLISH {
			pub = packet.NewPublishPacket()
			pub.Decode(tmp[0:n])
			pkt = &packet.PubackPacket{PacketID: pub.PacketID}
		} else {
			fmt.Println("tcp buffer:", l, ty)
			continue
		}

		dst := make([]byte, 100)
		n2, err := pkt.Encode(dst)
		if err != nil {
			fmt.Println("encode error:", err)
			break
		}
		if _, err = conn.Write(dst[0:n2]); err != nil {
			break
		}
		if pub != nil {
			resultingPubMessage = pub.Message.String()
			resultingPubTopic = pub.Message.Topic
			return true
		}
	}
	return false
}

func dummyMQTTD() {
	l, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", testPort))
	if err != nil {
		panic(err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		if readStream(conn) {
			wg.Done()
			return
		}
		conn.Close()
	}
	return
}

func TestSession(t *testing.T) {
	// Fake server to capture event
	wg.Add(1)
	go dummyMQTTD()
	SetMQTTHostPort("localhost", testPort, false)

	dc := &DeviceContext{
		DeviceID:  0,             // change this to real device ID
		AuthToken: "XXXXXXXXXXX", // change this to real API key assigned to device
	}

	if err := StartSession(dc); err != nil {
		fmt.Printf("Failed to start session: %v\n", err)
		os.Exit(1)
	}

	data := []byte("blob")
	SendBulkData("stream1", data, QOSAtLeastOnce)
	wg.Wait()
	StopSession()
	assert.Equal(t, resultingPubTopic, "/devices/0/bulkData/stream1")
}
