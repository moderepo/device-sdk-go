package mode

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	packet "github.com/moderepo/device-sdk-go/mqtt_packet"
	"github.com/stretchr/testify/assert"
)

var (
	sessionTestPort int            = 9999
	sessionMqttTest MqttTestDaemon = MqttTestDaemon{
		ExitChannel:  make(chan bool, 1),
		Results:      map[string]string{},
		TestPort:     sessionTestPort,
		WaitOn:       map[string]bool{},
		WriteChannel: make(chan packet.PublishPacket, 1),
	}
)

func TestMain(m *testing.M) {
	SetMQTTHostPort("localhost", sessionTestPort, false)
	go sessionMqttTest.Start()
	exitCode := m.Run()
	sessionMqttTest.ShutDown()
	os.Exit(exitCode)
}

func TestSessionSendBulkData(t *testing.T) {
	sessionMqttTest.WaitOn["pub"] = true
	sessionMqttTest.WG.Add(1)

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
	sessionMqttTest.WG.Wait()
	StopSession()
	assert.Equal(t, sessionMqttTest.Results["PubTopic"], "/devices/0/bulkdata/stream1")
}

func TestSessionSystemCommandNoFunc(t *testing.T) {
	sessionMqttTest.Reset()
	sessionMqttTest.WaitOn["writeStream"] = true
	sessionMqttTest.WG.Add(1)

	dc := &DeviceContext{
		DeviceID:  0,             // change this to real device ID
		AuthToken: "XXXXXXXXXXX", // change this to real API key assigned to device
	}

	if err := StartSession(dc); err != nil {
		fmt.Printf("Failed to start session: %v\n", err)
		os.Exit(1)
	}

	payloadStr := `{"action":"blah", "parameters":{"a":"b"}}`
	payload := bytes.NewBufferString(payloadStr).Bytes()
	sessionMqttTest.WriteChannel <- packet.PublishPacket{
		Message: packet.Message{
			Topic:   "/devices/0/systemcommand",
			Payload: payload,
			QOS:     packet.QOSAtLeastOnce,
		},
		PacketID: 10,
	}

	sessionMqttTest.WG.Wait()
	StopSession()
	assert.Equal(t, true, true)
}

func TestSessionSystemCommand(t *testing.T) {
	sessionMqttTest.Reset()
	sessionMqttTest.WaitOn["writeStream"] = true
	sessionMqttTest.WG.Add(2)

	dc := &DeviceContext{
		DeviceID:  0,             // change this to real device ID
		AuthToken: "XXXXXXXXXXX", // change this to real API key assigned to device
	}

	var action string
	SetSystemCommandHandler("blah", func(ctx *DeviceContext, cmd *DeviceCommand) {
		action = cmd.Action
		sessionMqttTest.WG.Done()
	})

	if err := StartSession(dc); err != nil {
		fmt.Printf("Failed to start session: %v\n", err)
		os.Exit(1)
	}

	payloadStr := `{"action":"blah", "parameters":{"a":"b"}}`
	payload := bytes.NewBufferString(payloadStr).Bytes()
	sessionMqttTest.WriteChannel <- packet.PublishPacket{
		Message: packet.Message{
			Topic:   "/devices/0/systemcommand",
			Payload: payload,
			QOS:     packet.QOSAtLeastOnce,
		},
		PacketID: 10,
	}

	sessionMqttTest.WG.Wait()
	StopSession()
	assert.Equal(t, action, "blah")
}
