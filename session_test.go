package mode

import (
	"fmt"
	"io"

	//	"io/ioutil"
	"net"
	"sync"
	"testing"

	packet "github.com/moderepo/device-sdk-go/mqtt_packet"
	"github.com/stretchr/testify/assert"
)

var testPort = 9999
var wg sync.WaitGroup

func readStream(conn net.Conn, pubCallback func(*packet.PublishPacket) bool) bool {
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
		var pub *packet.PublishPacket
		if ty == packet.CONNECT {
			pkt = &packet.ConnackPacket{ReturnCode: packet.ConnectionAccepted}
		} else if ty == packet.SUBSCRIBE {
			pkt = &packet.SubackPacket{PacketID: 1, ReturnCodes: []byte{packet.QOSAtMostOnce, packet.QOSAtMostOnce}}
		} else if ty == packet.PUBLISH {
			pub = packet.NewPublishPacket()
			pub.Decode(tmp[0:l])
			if pubCallback != nil && !pubCallback(pub) {
				return true
			}
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
			return true
		}
	}
	return false
}

func dummyMQTTD(pubCallback func(*packet.PublishPacket) bool) {
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
		if readStream(conn, pubCallback) {
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

	var actual string
	go dummyMQTTD(func(pub *packet.PublishPacket) bool {
		actual = pub.Message.Topic
		return true
	})
	SetMQTTHostPort("localhost", testPort, false)

	dc := &DeviceContext{
		DeviceID:  0,             // change this to real device ID
		AuthToken: "XXXXXXXXXXX", // change this to real API key assigned to device
	}

	if err := StartSession(dc); err != nil {
		t.Fatalf("Failed to start session: %v\n", err)
		return
	}

	data := []byte("blob")
	SendBulkData("stream1", data, QOSAtLeastOnce)
	wg.Wait()
	StopSession()
	assert.Equal(t, actual, "/devices/0/bulkData/stream1")
}

func TestWriteBulkData(t *testing.T) {
	t.Run("Synchronize write to TSDB", func(t *testing.T) {
		// Fake server to capture event
		wg.Add(1)

		var actual string
		go dummyMQTTD(func(pub *packet.PublishPacket) bool {
			actual = pub.Message.Topic
			return true
		})
		SetMQTTHostPort("localhost", testPort, false)

		dc := &DeviceContext{
			DeviceID:  0,             // change this to real device ID
			AuthToken: "XXXXXXXXXXX", // change this to real API key assigned to device
		}

		if err := StartSession(dc); err != nil {
			t.Fatalf("Failed to start session: %v\n", err)
			return
		}

		data := []byte("blob")
		err := WriteBulkData("stream1", data)
		wg.Wait()
		StopSession()
		assert.Equal(t, actual, "/devices/0/bulkData/stream1")
		assert.NoError(t, err)
	})

	t.Run("When nothing responds until the TSDB server timeout, return an error.", func(t *testing.T) {
		// Fake server to capture event
		wg.Add(1)

		var actual string
		go dummyMQTTD(func(pub *packet.PublishPacket) bool {
			actual = pub.Message.Topic
			return false
		})
		SetMQTTHostPort("localhost", testPort, false)

		dc := &DeviceContext{
			DeviceID:  0,             // change this to real device ID
			AuthToken: "XXXXXXXXXXX", // change this to real API key assigned to device
		}

		if err := StartSession(dc); err != nil {
			t.Fatalf("Failed to start session: %v\n", err)
			return
		}

		data := []byte("blob")
		err := WriteBulkData("stream1", data)
		wg.Wait()
		StopSession()
		assert.Equal(t, actual, "/devices/0/bulkData/stream1")
		assert.Error(t, err)
	})
}
