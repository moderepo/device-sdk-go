package mode

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

type TestMqttDelegate struct {
	username        string
	password        string
	subscriptions   []string
	subRecvCh       chan MqttSubData
	queueAckCh      chan MqttResponse
	pingAckCh       chan MqttResponse
	responseTimeout time.Duration
}

func newTestMqttDelegate() *TestMqttDelegate {
	return &TestMqttDelegate{
		username:  "good",
		password:  "anything",
		subRecvCh: make(chan MqttSubData),
		// in practice, this would be buffered
		queueAckCh:      make(chan MqttResponse),
		pingAckCh:       make(chan MqttResponse, 2),
		responseTimeout: 2 * time.Second,
	}
}

func invalidTestMqttDelegate() TestMqttDelegate {
	return TestMqttDelegate{
		username:   "foo",
		password:   "bar",
		subRecvCh:  make(chan MqttSubData),
		queueAckCh: make(chan MqttResponse),
		pingAckCh:  make(chan MqttResponse),
	}
}

func (del TestMqttDelegate) TLSUsageAndConfiguration() (useTLS bool,
	tlsConfig *tls.Config) {
	return useTLS, nil
}

func (del TestMqttDelegate) AuthInfo() (username string, password string) {
	return del.username, del.password
}

func (del TestMqttDelegate) GetChannels() (chan<- MqttSubData,
	chan MqttResponse, chan MqttResponse) {
	return del.subRecvCh, del.queueAckCh, del.pingAckCh
}

func (del TestMqttDelegate) OutgoingQueueSize() uint16 {
	return 2
}

func (del TestMqttDelegate) GetSubscriptions() []string {
	// could do this in the initializer, but then we can't bind it to this
	// instance of the delegate
	del.subscriptions = []string{
		fmt.Sprintf("/devices/%s/command", del.username),
		fmt.Sprintf("/devices/%s/kv", del.username),
	}
	return del.subscriptions
}

func (del TestMqttDelegate) OnClose() {
	// Nothing to do
}

func (del TestMqttDelegate) createContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), del.responseTimeout)
}

func testConnection(ctx context.Context, t *testing.T, delegate MqttDelegate,
	expectError bool) *MqttClient {

	client := NewMqttClient(testMqttHost, testMqttPort, delegate)
	err := client.Connect(ctx)
	if expectError {
		if err == nil {
			t.Errorf("Did not receive expected error")
		} else {
			fmt.Printf("Received expected error: %s\n", err)
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
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, nil)
	fmt.Println("TestMqttClientConnction: test bad user/pass")
	badDelegate := invalidTestMqttDelegate()
	fmt.Println("Hitting server")
	testConnection(ctx, t, badDelegate, true)

	fmt.Println("TestMqttClientConnction: test good user/pass")
	goodDelegate := newTestMqttDelegate()
	//client := testConnection(t, goodDelegate, false)
	client := testConnection(ctx, t, goodDelegate, false)

	if client.Disconnect(ctx) != nil {
		t.Errorf("error disconnecting")
	}

	cancel()
	wg.Wait()
}

func TestMqttClientSubscribe(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, nil)
	fmt.Println("TestMqttClientSubscribe")
	goodDelegate := newTestMqttDelegate()
	client := testConnection(ctx, t, goodDelegate, false)
	if errs := client.Subscribe(ctx); errs != nil {
		t.Errorf("failed to subscribe: %s", errs)
	}

	if client.Disconnect(ctx) != nil {
		t.Errorf("error disconnecting")
	}
	cancel()
	wg.Wait()
}

func TestMqttClientPublish(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, nil)
	fmt.Println("TestMqttClientPublish")
	goodDelegate := newTestMqttDelegate()
	client := testConnection(ctx, t, goodDelegate, false)

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
	packetId, err := client.Publish(ctx, event.Qos, topic, rawData)
	if err != nil {
		t.Errorf("Publish send failed: %s", err)
	}

	// Wait for the ack.
	ackData := <-goodDelegate.queueAckCh
	if ackData.Err != nil {
		t.Errorf("Publish ack failed: %s", ackData.Err)
	}
	if packetId != ackData.PacketID {
		t.Errorf("Publish: Id and ack id do not match: %d, %d",
			packetId, ackData.PacketID)
	}

	if client.Disconnect(ctx) != nil {
		t.Errorf("error disconnecting")
	}
	cancel()
	wg.Wait()
}

func sendPing(ctx context.Context, t *testing.T, client *MqttClient,
	del *TestMqttDelegate) error {
	if err := client.Ping(ctx); err != nil {
		return err
	}

	// Block on the return channel or timeout
	select {
	case resp := <-del.pingAckCh:
		if resp.Err != nil {
			// This is always an error
			t.Errorf("Ping response returned error: %s", resp.Err)
		}
	case <-ctx.Done():
		// If we're testing timeouts, this is expected. Not always test
		// failure
		return fmt.Errorf("Timed out waiting for ping response")
	}

	return nil
}

func TestMqttClientPing(t *testing.T) {
	fmt.Println("TestMqttClientPing")
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, nil)

	goodDelegate := newTestMqttDelegate()
	client := testConnection(ctx, t, goodDelegate, false)

	err := sendPing(ctx, t, client, goodDelegate)
	if err != nil {
		t.Errorf("ping failed")
	}

	if client.Disconnect(ctx) != nil {
		t.Errorf("error disconnecting")
	}
	logInfo("Cancel to server to finish")
	cancel()
	wg.Wait()
}

func TestMqttClientPingWithTimeout(t *testing.T) {
	fmt.Println("TestMqttClientPingWithTimeout")
	var wg sync.WaitGroup
	wg.Add(1)
	cmdCh := make(chan dummyCmd)
	dummyMQTTD(nil, &wg, cmdCh)

	goodDelegate := newTestMqttDelegate()
	ctx, cancel := goodDelegate.createContext()
	defer cancel()
	client := testConnection(ctx, t, goodDelegate, false)

	cmdCh <- slowdownServerCmd
	// Wait for the ack, or timeout
	err := sendPing(ctx, t, client, goodDelegate)
	if err != nil {
		logInfo("Received expected error: %s", err)
	} else {
		t.Errorf("Ping succeeded but should have timed out")
	}

	// This apparently closes our server connection, so we won't disconnect
	// but in the real case, the connection might just be slow

	logInfo("Sending cancel to server")
	cmdCh <- shutdownCmd
	wg.Wait()
}
