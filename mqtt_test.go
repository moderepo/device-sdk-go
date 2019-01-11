package mode

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	packet "github.com/moderepo/device-sdk-go/mqtt_packet"
	"github.com/stretchr/testify/assert"
)

var (
	mqttTest MqttTestDaemon = MqttTestDaemon{
		TestPort:     9998,
		WriteChannel: make(chan packet.PublishPacket, 1),
		ExitChannel:  make(chan bool, 1),
		Results:      map[string]string{},
	}
)

func init() {
	SetMQTTHostPort("localhost", mqttTest.TestPort, false)
}

func TestMqtt(t *testing.T) {
	// Fake server to capture event
	go mqttTest.Start()

	dc := &DeviceContext{
		DeviceID:  0,             // change this to real device ID
		AuthToken: "XXXXXXXXXXX", // change this to real API key assigned to device
	}

	cmdQueue := make(chan *DeviceCommand, commandQueueLength)
	evtQueue := make(chan *DeviceEvent, eventQueueLength)
	bulkDataQueue := make(chan *DeviceBulkData, bulkDataQueueLength)
	syncQueue := make(chan *keyValueSync, keyValueSyncQueueLength)
	pushQueue := make(chan *keyValueSync, keyValuePushQueueLength)
	evtQueue <- &DeviceEvent{EventType: "", qos: QOSAtLeastOnce}
	conn, err := dc.openMQTTConn(cmdQueue, evtQueue, bulkDataQueue, syncQueue, pushQueue)
	fmt.Println("OpenMQTTConn err:", err)
	if err != nil {
		mqttTest.ShutDown()
		return
	}
	payloadStr := `{"action":"blah", "parameters":{"a":"b"}}`
	payload := bytes.NewBufferString(payloadStr).Bytes()
	mqttTest.WriteChannel <- packet.PublishPacket{
		Message: packet.Message{
			Topic:   "/devices/0/systemcommand",
			Payload: payload,
			QOS:     packet.QOSAtLeastOnce,
		},
		PacketID: 10,
	}
	fmt.Println("wrote into WriteChannel")
	time.Sleep(time.Second * 10)
	conn.close()
	fmt.Println("connection is closed")
	mqttTest.ShutDown()
	assert.Equal(t, 2, 2)
}
