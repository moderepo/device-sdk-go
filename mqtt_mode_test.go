package mode

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

var (
	// My cloudmqtt.com server)
	modeMqttHost = "localhost"
	modeMqttPort = 1998
	modeUseTLS   = false
)

func invalidModeMqttDelegate() (*ModeMqttDelegate, chan *DeviceCommand,
	chan *KeyValueSync) {
	dc := &DeviceContext{
		DeviceID:  1,
		AuthToken: "XXXX",
	}
	cmdQueue := make(chan *DeviceCommand)
	kvSyncQueue := make(chan *KeyValueSync)

	return NewModeMqttDelegate(dc, cmdQueue, kvSyncQueue), cmdQueue, kvSyncQueue
}

func newModeMqttDelegate() (*ModeMqttDelegate, chan *DeviceCommand,
	chan *KeyValueSync) {
	dc := &DeviceContext{
		DeviceID:  1234,
		AuthToken: "v1.XXXXXXXXX",
	}

	cmdQueue := make(chan *DeviceCommand, 1)
	kvSyncQueue := make(chan *KeyValueSync, 1)
	return NewModeMqttDelegate(dc, cmdQueue, kvSyncQueue), cmdQueue, kvSyncQueue
}

// Helper for some tests
func testModeConnection(t *testing.T, delegate MqttDelegate,
	expectError bool) *MqttClient {
	client := NewMqttClient(modeMqttHost, modeMqttPort, nil, modeUseTLS,
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

// Helper for some tests
func testModeWaitForPubAck(t *testing.T, delegate *ModeMqttDelegate,
	expectTimeout bool) uint16 {
	// Wait for the ack, or timeout
	ctx, cancel := context.WithTimeout(context.Background(),
		delegate.RequestTimeout())
	defer cancel()

	// Block on the return channel or timeout
	select {
	case queueRes := <-delegate.QueueAckCh:
		if queueRes.Err != nil {
			t.Errorf("Queued request failed: %s", queueRes.Err)
		} else {
			return queueRes.PacketId
		}
	case <-ctx.Done():
		if !expectTimeout {
			t.Errorf("Ack response timeout: %s", ctx.Err())
		} else {
			return 0
		}
	}

	return 0
}

func TestModeMqttClientConnection(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, publishNone)

	fmt.Println("TestModeMqttClientConnection: test bad user/pass")
	badDelegate, _, _ := invalidModeMqttDelegate()
	testModeConnection(t, badDelegate, true)

	fmt.Println("TestModeMqttClientConnection: test good user/pass")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(t, goodDelegate, false)

	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
	cancel()
	wg.Wait()
}

func TestModeMqttClientSubscribe(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, publishNone)

	fmt.Println("TestModeMqttClientSubscribe")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(t, goodDelegate, false)
	if err := client.Subscribe(); err != nil {
		t.Errorf("failed to subscribe: %s", err)
	}

	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
	cancel()
	wg.Wait()
}

func TestModeMqttClientPing(t *testing.T) {
	var wg sync.WaitGroup
	serverCtx, serverCancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(serverCtx, &wg, publishNone)

	fmt.Println("TestMqttClientPing")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(t, goodDelegate, false)
	if !client.IsConnected() {
		t.Errorf("Lost connection")
	}

	if err := client.Ping(); err != nil {
		t.Errorf("Ping send failed: %s", err)
	}

	// Wait for the ack, or timeout
	ctx, cancel := context.WithTimeout(context.Background(),
		client.delegate.RequestTimeout())
	defer cancel()

	// Block on the return channel or timeout
	select {
	case ret := <-goodDelegate.PingAckCh:
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
	serverCancel()
	wg.Wait()
}

func TestModeMqttClientPublishEvent(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, publishNone)

	fmt.Println("TestMqttClientPublishEvent")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(t, goodDelegate, false)
	if !client.IsConnected() {
		t.Errorf("Lost connection")
	}

	event := DeviceEvent{
		EventType: "TestEvent",
		EventData: map[string]interface{}{"key1": "val1"},
		Qos:       QOSAtMostOnce,
	}

	fmt.Println("Testing timeout for QOS 0")
	var packetId uint16
	packetId, err := client.PublishEvent(event)
	if err != nil {
		t.Errorf("Publish failed: %s", err)
	}

	returnId := testModeWaitForPubAck(t, goodDelegate, true)
	if returnId != 0 {
		t.Errorf("Received an Ack for QOS 0")
	}

	fmt.Println("Testing ACK for QOS 1")
	event.Qos = QOSAtLeastOnce
	if packetId, err = client.PublishEvent(event); err != nil {
		t.Errorf("Publish failed: %s", err)
	}

	returnId = testModeWaitForPubAck(t, goodDelegate, false)

	if packetId != returnId {
		t.Errorf("Send packet %d did not match Ack packet %d\n",
			packetId, returnId)
	}

	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
	cancel()
	wg.Wait()
}

// Only test QOS 1 for this (QOS 0 was tested in PublishEvent, so, unless we
// hit a bug with BulkData publishing, I think that's sufficient.
func TestModeMqttClientPublishBulkData(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, publishNone)

	fmt.Println("TestMqttClientPublishBulkData")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(t, goodDelegate, false)
	if !client.IsConnected() {
		t.Errorf("Lost connection")
	}

	bulk := DeviceBulkData{
		StreamID: "stream1",
		Blob:     []byte("blob"),
		Qos:      QOSAtLeastOnce,
	}
	_, err := client.PublishBulkData(bulk)
	if err != nil {
		t.Errorf("PublishDeviceBulkData send failed: %s", err)
	}

	// From looking at the original session.go, I believe this was QOS0 (no
	// ACK. So, once we send it, we're done
	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
	cancel()
	wg.Wait()
}

func TestModeMqttClientPublishKVUpdate(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, publishNone)

	fmt.Println("TestMqttClientPublishKVUpdate")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(t, goodDelegate, false)
	if !client.IsConnected() {
		t.Errorf("Lost connection")
	}

	kvData := KeyValueSync{
		Action: KVSyncActionSet,
		Key:    "boo99",
		Value:  "far99",
	}

	// Set the value
	packetId, err := client.PublishKeyValueUpdate(kvData)
	if err != nil {
		t.Errorf("PublishKeyValueUpdate send failed: %s", err)
	}

	returnId := testModeWaitForPubAck(t, goodDelegate, false)
	if packetId != returnId {
		t.Errorf("Send packet %d did not match Ack packet %d\n",
			packetId, returnId)
	}

	// Reload the value
	kvData.Action = KVSyncActionReload

	packetId, err = client.PublishKeyValueUpdate(kvData)
	if err != nil {
		t.Errorf("PublishKeyValueUpdate send failed: %s", err)
	}
	returnId = testModeWaitForPubAck(t, goodDelegate, true)

	if packetId != returnId {
		t.Errorf("Send packet %d did not match Ack packet %d\n",
			packetId, returnId)
	}

	// Delete the value
	kvData.Action = KVSyncActionDelete

	packetId, err = client.PublishKeyValueUpdate(kvData)
	if err != nil {
		t.Errorf("PublishKeyValueUpdate send failed: %s", err)
	}

	returnId = testModeWaitForPubAck(t, goodDelegate, true)

	if packetId != returnId {
		t.Errorf("Send packet %d did not match Ack packet %d\n",
			packetId, returnId)
	}

	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
	cancel()
	wg.Wait()
}

// When we first connect and subscribe to the kv topic, we'll get a sync.
// This is a good test to see that we're receiving.
func TestModeMqttClientReceiveKvSync(t *testing.T) {
	var wg sync.WaitGroup
	serverCtx, serverCancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(serverCtx, &wg, publishKvUpdate)

	fmt.Println("TestModeMqttClientReceiveKvSync")
	goodDelegate, _, kvSyncChannel := newModeMqttDelegate()
	client := NewMqttClient(modeMqttHost, modeMqttPort, nil, modeUseTLS,
		goodDelegate)
	client.Connect()

	// Start the listener
	go goodDelegate.RunSubscriptionListener()

	if !client.IsConnected() {
		t.Errorf("Failed to connect")
	}

	if err := client.Subscribe(); err != nil {
		t.Errorf("failed to subscribe: %s", err)
	}

	// 3 seconds seems to be the correct timing for this test to cause the bulk
	// sync to be sent by the server
	d := time.Now().Add(3 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	receivedSubData := false
	select {
	case kvSync := <-kvSyncChannel:
		fmt.Printf("Received the sync from the server: %s\n", kvSync.Action)
		fmt.Printf("Data: %v\n", kvSync)
		receivedSubData = true
		break
	case <-ctx.Done():
		t.Errorf("Timed out waiting for command")
	}

	if !receivedSubData {
		t.Errorf("Did not receive KV Sync")
	}
	fmt.Println("Exiting")
	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
	serverCancel()
	wg.Wait()
}

// We need to wait for someone to send this to us, so I'm not sure how to
// initiate this automatically
func TestModeMqttClientReceiveCommand(t *testing.T) {
	var wg sync.WaitGroup
	serverCtx, serverCancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(serverCtx, &wg, publishCommand)

	fmt.Println("TestModeMqttClientReceiveCommand")
	goodDelegate, cmdChannel, _ := newModeMqttDelegate()
	client := NewMqttClient(modeMqttHost, modeMqttPort, nil, modeUseTLS,
		goodDelegate)
	client.Connect()

	// Start the listener
	go goodDelegate.RunSubscriptionListener()

	if !client.IsConnected() {
		t.Errorf("Failed to connect")
	}

	if err := client.Subscribe(); err != nil {
		t.Errorf("failed to subscribe: %s", err)
	}

	// 3 seconds seems to be the correct timing for this test to cause the bulk
	// sync to be sent by the server
	d := time.Now().Add(10 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()

	receivedSubData := false
	select {
	case cmd := <-cmdChannel:
		fmt.Printf("Received the command from the server: %s\n", cmd.Action)
		receivedSubData = true
		break
	case <-ctx.Done():
		t.Errorf("Timed out waiting for command")
	}

	if !receivedSubData {
		t.Errorf("Did not receive command")
	}
	fmt.Println("Exiting")
	if client.Disconnect() != nil {
		t.Errorf("error disconnecting")
	}
	serverCancel()
	wg.Wait()
}
