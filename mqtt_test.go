package mode

import (
	"encoding/json"
	"errors"
	"fmt"
	packet "github.com/moderepo/device-sdk-go/v2/mqtt_packet"
	"testing"
	"time"
)

var (
	// My cloudmqtt.com server)
	testMqttHost   = "tailor.cloudmqtt.com"
	testMqttPort   = 28371
	useTLS         = true
	requestTimeout = 4 * time.Second
	pingInterval   = 4 * time.Second
	noPingInterval = time.Duration(0)

	subscriptions []MqttSubscription
)

type TestMqttDelegate struct {
	username      string
	password      string
	subscriptions []MqttSubscription
}

func newTestMqttDelegate() TestMqttDelegate {
	return TestMqttDelegate{
		// This is a user level password, which I can change, but it still shouldn't be in
		// here. But, how else do you do automated tests in a public server?
		username: "wcuyfgii",
		password: "PrQge1s_AVVb",
	}
}

func invalidTestMqttDelegate() TestMqttDelegate {
	return TestMqttDelegate{
		username: "foo",
		password: "bar",
	}
}

func (del TestMqttDelegate) AuthInfo() (username string, password string) {
	return del.username, del.password
}

func (del TestMqttDelegate) BuildPublishPacket(data interface{}) (packet.Packet, error) {
	var (
		qos   byte
		event DeviceEvent
		ok    bool
	)
	if event, ok = data.(DeviceEvent); !ok {
		return nil, errors.New("unexpected event type")
	}

	switch event.qos {
	case QOSAtMostOnce:
		qos = packet.QOSAtMostOnce
	case QOSAtLeastOnce:
		qos = packet.QOSAtLeastOnce
	default:
		return nil, errors.New("unsupported qos level")
	}

	payload, _ := json.Marshal(event)
	fmt.Printf("Payload: [%s]\n", payload)
	p := packet.NewPublishPacket()
	p.Message = packet.Message{
		Topic:   fmt.Sprintf("/devices/%s/event", del.username),
		QOS:     qos,
		Payload: payload,
	}
	logInfo("Publishing on topic [%s]", p.Message.Topic)

	return p, nil
}

func (del TestMqttDelegate) Subscriptions() []MqttSubscription {
	// could do this in the initializer, but then we can't bind it to this instance of
	// the delegate
	del.subscriptions = []MqttSubscription{
		MqttSubscription{
			topic:      fmt.Sprintf("/devices/%s/command", del.username),
			msgHandler: del.handleCommandMsg,
		},
		MqttSubscription{
			topic:      fmt.Sprintf("/devices/%s/kv", del.username),
			msgHandler: del.handleKeyValueMsg,
		},
	}
	return del.subscriptions
}

func (del TestMqttDelegate) handleCommandMsg(*packet.PublishPacket) error {
	return nil
}

func (del TestMqttDelegate) handleKeyValueMsg(*packet.PublishPacket) error {
	return nil
}

func testConnection(t *testing.T, delegate MqttDelegate,
	expectError bool) *MqttClient {
	client := NewMqttClient(testMqttHost, testMqttPort, nil, useTLS,
		requestTimeout, noPingInterval, delegate)
	err := client.Connect()
	if expectError {
		if err == nil {
			t.Errorf("Did not receive expected error")
		}
	} else if err != nil {
		t.Errorf("Connect failed: %s", err)
	}
	isConnected := client.IsConnected()
	if expectError {
		if isConnected {
			t.Errorf("IsConnected should not be true")
		}
	} else if !isConnected {
		t.Errorf("IsConnected is false after connection")
	}

	return client
}

func testMqttClientConnection(t *testing.T) {
	fmt.Println("TestMqttClientConnction: test bad user/pass")
	badDelegate := invalidTestMqttDelegate()
	testConnection(t, badDelegate, true)

	fmt.Println("TestMqttClientConnction: test good user/pass")
	goodDelegate := newTestMqttDelegate()
	client := testConnection(t, goodDelegate, false)

	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
}

func testMqttClientSubscribe(t *testing.T) {
	fmt.Println("TestMqttClientSubscribe")
	goodDelegate := newTestMqttDelegate()
	client := testConnection(t, goodDelegate, false)
	if err := client.AddSubscriptions(); err != nil {
		t.Errorf("failed to subscribe: %s", err)
	}

	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
}

func TestMqttClientPublish(t *testing.T) {
	fmt.Println("TestMqttClientPublish")
	goodDelegate := newTestMqttDelegate()
	client := testConnection(t, goodDelegate, false)

	eventData := make(map[string]interface{})
	eventData["key1"] = "val1"
	event := DeviceEvent{"CustomEvent", eventData, QOSAtLeastOnce}
	if err := client.Publish(event); err != nil {
		t.Errorf("Publish failed: %s", err)
	}

	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
}

func TestMqttClientPingLoop(t *testing.T) {
	fmt.Println("TestMqttClientPingLoop")
	// Ping every 10 milliseconds
	client := NewMqttClient(testMqttHost, testMqttPort, nil, useTLS,
		requestTimeout, 10*time.Millisecond, newTestMqttDelegate())
	client.Connect()
	if !client.IsConnected() {
		t.Errorf("Failed to connect")
	}
	time.Sleep(50 * time.Millisecond)
	if !client.IsConnected() {
		t.Errorf("Lost connection")
	}

	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
}
