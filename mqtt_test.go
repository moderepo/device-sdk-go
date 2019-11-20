package mode_client

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

var (
	// My cloudmqtt.com server)
	testMqttHost   = "tailor.cloudmqtt.com"
	testMqttPort   = 28371
	useTLS         = true
	requestTimeout = 2 * time.Second
)

type TestMqttDelegate struct {
	username      string
	password      string
	subscriptions []string
	subRecvCh     chan MqttSubData
	queueAckCh    chan MqttQueueResult
	pingAckCh     chan bool
}

func newTestMqttDelegate() TestMqttDelegate {
	return TestMqttDelegate{
		// This is a user level password, which I can change, but it still
		// shouldn't be in here. But, how else do you do automated tests in a
		// public server?
		username:   "wcuyfgii",
		password:   "PrQge1s_AVVb",
		subRecvCh:  make(chan MqttSubData),
		queueAckCh: make(chan MqttQueueResult),
		pingAckCh:  make(chan bool),
	}
}

func invalidTestMqttDelegate() TestMqttDelegate {
	return TestMqttDelegate{
		username:   "foo",
		password:   "bar",
		subRecvCh:  make(chan MqttSubData),
		queueAckCh: make(chan MqttQueueResult),
		pingAckCh:  make(chan bool),
	}
}

func (del TestMqttDelegate) AuthInfo() (username string, password string) {
	return del.username, del.password
}

func (del TestMqttDelegate) ReceiveChannels() (chan<- MqttSubData,
	chan<- MqttQueueResult, chan<- bool) {
	return del.subRecvCh, del.queueAckCh, del.pingAckCh
}

func (del TestMqttDelegate) RequestTimeout() time.Duration {
	return requestTimeout
}

func (del TestMqttDelegate) Subscriptions() []string {
	// could do this in the initializer, but then we can't bind it to this
	// instance of the delegate
	del.subscriptions = []string{
		fmt.Sprintf("/devices/%s/command", del.username),
		fmt.Sprintf("/devices/%s/kv", del.username),
	}
	return del.subscriptions
}

func (del TestMqttDelegate) Close() {
	// Nothing to do
}

func testConnection(t *testing.T, delegate MqttDelegate,
	expectError bool) *MqttClient {
	client := NewMqttClient(testMqttHost, testMqttPort, nil, useTLS,
		delegate)
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

func TestMqttClientConnection(t *testing.T) {
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

func TestMqttClientSubscribe(t *testing.T) {
	fmt.Println("TestMqttClientSubscribe")
	goodDelegate := newTestMqttDelegate()
	client := testConnection(t, goodDelegate, false)
	if err := client.Subscribe(); err != nil {
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

	topic := fmt.Sprintf("/devices/%s/event", goodDelegate.username)
	event := DeviceEvent{
		EventType: "CustomEvent",
		EventData: map[string]interface{}{"key1": "val1"},
		Qos:       QOSAtLeastOnce,
	}
	rawData, err := json.Marshal(event)
	if err != nil {
		t.Errorf("Publish send failed: %s", err)
	}
	packetId, err := client.Publish(event.Qos, topic, rawData)
	if err != nil {
		t.Errorf("Publish send failed: %s", err)
	}

	// Wait for the ack.
	ackData := <-goodDelegate.queueAckCh
	if ackData.Err != nil {
		t.Errorf("Publish ack failed: %s", ackData.Err)
	}
	if packetId != ackData.PacketId {
		t.Errorf("Publish: Id and ack id do not match: %d, %d",
			packetId, ackData.PacketId)
	}

	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
}

func TestMqttClientPing(t *testing.T) {
	fmt.Println("TestMqttClientPing")
	goodDelegate := newTestMqttDelegate()
	client := testConnection(t, goodDelegate, false)

	if err := client.Ping(); err != nil {
		t.Errorf("Ping send failed: %s", err)
	}

	// Wait for the ack, or timeout
	ctx, cancel := context.WithTimeout(context.Background(),
		client.delegate.RequestTimeout())
	defer cancel()

	// Block on the return channel or timeout
	select {
	case ret := <-goodDelegate.pingAckCh:
		// There's not really a way to return false, but, since it's bool, we'll
		// check
		if !ret {
			t.Errorf("Ping response was false")
		}
	case <-ctx.Done():
		t.Errorf("Ping response timeout: %s", ctx.Err())
	}

	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
}
