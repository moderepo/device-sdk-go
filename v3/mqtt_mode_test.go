package mode

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
func testModeConnection(ctx context.Context, t *testing.T,
	delegate *ModeMqttDelegate, expectError bool) *MqttClient {
	client := NewMqttClient(modeMqttHost, modeMqttPort,
		WithMqttDelegate(delegate))
	ctx, cancel := delegate.createContext()
	defer cancel()
	err := client.Connect(ctx)
	if expectError {
		assert.NotNil(t, err, "Did not receive expected error")
	} else {
		assert.Nil(t, err, "Connect failed")
	}
	isConnected := client.IsConnected()
	if expectError {
		assert.False(t, isConnected, "IsConnected should not be true")
	} else {
		assert.True(t, isConnected, "IsConnected is false after connection")
	}

	return client
}

// Helper for some tests
func testModeWaitForPubAck(t *testing.T, delegate *ModeMqttDelegate,
	expectTimeout bool) uint16 {
	// Wait for the ack, or timeout
	ctx, cancel := delegate.createContext()
	defer cancel()

	// Block on the return channel or timeout
	select {
	case queueResp := <-delegate.QueueAckCh:
		assert.Nil(t, queueResp.Err, "Queued request failed")
		return queueResp.PacketID
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
	dummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestModeMqttClientConnection: test bad user/pass")
	badDelegate, _, _ := invalidModeMqttDelegate()
	testModeConnection(ctx, t, badDelegate, true)

	fmt.Println("TestModeMqttClientConnection: test good user/pass")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)

	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	// force the cancel before the wait
	cancel()
	wg.Wait()
}

func TestModeMqttClientSubscribe(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestModeMqttClientSubscribe")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)
	errs := client.Subscribe(ctx)
	assert.Nil(t, errs, "failed to subscribe")

	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

func TestModeMqttClientPing(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientPing")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)
	assert.True(t, client.IsConnected(), "Lost connection")

	assert.Nil(t, client.Ping(ctx), "Ping send failed")

	// Block on the return channel or timeout
	select {
	case resp := <-goodDelegate.PingAckCh:
		assert.Nil(t, resp.Err, "Ping response returned error")
	case <-ctx.Done():
		t.Errorf("Ping response timeout: %s", ctx.Err())
	}

	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

func TestModeMqttClientPublishEvent(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientPublishEvent")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)
	assert.True(t, client.IsConnected(), "Lost connection")

	event := DeviceEvent{
		EventType: "TestEvent",
		EventData: map[string]interface{}{"key1": "val1"},
		Qos:       QOSAtMostOnce,
	}

	var packetID uint16
	packetID, err := client.PublishEvent(ctx, event)
	assert.Nil(t, err, "Publish failed")

	fmt.Println("QOS 0. No ACK, so make sure we don't get one")
	returnID := testModeWaitForPubAck(t, goodDelegate, true)
	assert.Equal(t, returnID, uint16(0), "Received an Ack for QOS 0")

	fmt.Println("Testing ACK for QOS 1")
	event.Qos = QOSAtLeastOnce
	packetID, err = client.PublishEvent(ctx, event)
	assert.Nil(t, err, "Publish failed")

	returnID = testModeWaitForPubAck(t, goodDelegate, false)
	assert.Equal(t, returnID, packetID, "Send packet did not match Ack packet")

	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

// Only test QOS 1 for this (QOS 0 was tested in PublishEvent, so, unless we
// hit a bug with BulkData publishing, I think that's sufficient.
func TestModeMqttClientPublishBulkData(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientPublishBulkData")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)
	assert.True(t, client.IsConnected(), "Lost connection")

	bulk := DeviceBulkData{
		StreamID: "stream1",
		Blob:     []byte("blob"),
		Qos:      QOSAtMostOnce,
	}
	_, err := client.PublishBulkData(ctx, bulk)
	assert.Nil(t, err, "PublishDeviceBulkData send failed")

	logInfo("[MQTT Test] disconnecting")
	// From looking at the original session.go, I believe this was QOS0 (no
	// ACK. So, once we send it, we're done
	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

func TestModeMqttClientPublishKVUpdate(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	dummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientPublishKVUpdate")
	goodDelegate, _, _ := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)
	assert.True(t, client.IsConnected(), "Lost connection")

	kvData := KeyValueSync{
		Action: KVSyncActionSet,
		Key:    "boo99",
		Value:  "far99",
	}

	// Set the value
	packetID, err := client.PublishKeyValueUpdate(ctx, kvData)
	assert.Nil(t, err, "PublishKeyValueUpdate send failed")

	returnID := testModeWaitForPubAck(t, goodDelegate, false)
	assert.Equal(t, returnID, packetID, "Send packet did not match Ack packet")

	// Reload the value
	kvData.Action = KVSyncActionReload

	packetID, err = client.PublishKeyValueUpdate(ctx, kvData)
	assert.Nil(t, err, "PublishKeyValueUpdate send failed")

	returnID = testModeWaitForPubAck(t, goodDelegate, true)
	assert.Equal(t, returnID, packetID, "Send packet did not match Ack packet")

	// Delete the value
	kvData.Action = KVSyncActionDelete

	packetID, err = client.PublishKeyValueUpdate(ctx, kvData)
	assert.Nil(t, err, "PublishKeyValueUpdate send failed")

	returnID = testModeWaitForPubAck(t, goodDelegate, true)
	assert.Equal(t, returnID, packetID, "Send packet did not match Ack packet")

	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

// When we first connect and subscribe to the kv topic, we'll get a sync.
// This is a good test to see that we're receiving.
func TestModeMqttClientReceiveKVSync(t *testing.T) {
	var wg sync.WaitGroup
	cmdCh := make(chan dummyCmd)
	wg.Add(1)
	dummyMQTTD(nil, &wg, cmdCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("TestModeMqttClientReceiveKvSync")
	goodDelegate, _, kvSyncChannel := newModeMqttDelegate()
	client := NewMqttClient(modeMqttHost, modeMqttPort,
		WithMqttDelegate(goodDelegate))
	client.Connect(ctx)
	// Tell the server to send a kv update
	cmdCh <- publishKvCmd
	// Start the listener
	goodDelegate.StartSubscriptionListener()

	assert.True(t, client.IsConnected(), "Lost connection")
	assert.Nil(t, client.Subscribe(ctx), "failed to subscribe")

	d := time.Now().Add(2 * time.Second)
	syncCtx, syncCancel := context.WithDeadline(context.Background(), d)
	defer syncCancel()

	receivedSubData := false
	select {
	case kvSync := <-kvSyncChannel:
		fmt.Printf("Received the sync from the server: %s\n", kvSync.Action)
		fmt.Printf("Data: %v\n", kvSync)
		receivedSubData = true
		break
	case <-syncCtx.Done():
		t.Errorf("Timed out waiting for command")
	}

	assert.True(t, receivedSubData, "Did not receive KV Sync")

	fmt.Println("Exiting")
	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cmdCh <- shutdownCmd
	wg.Wait()
}

// We need to wait for someone to send this to us, so I'm not sure how to
// initiate this automatically
func TestModeMqttClientReceiveCommand(t *testing.T) {
	var wg sync.WaitGroup
	cmdCh := make(chan dummyCmd)
	wg.Add(1)
	dummyMQTTD(nil, &wg, cmdCh)

	fmt.Println("TestModeMqttClientReceiveCommand")
	goodDelegate, cmdChannel, _ := newModeMqttDelegate()
	client := NewMqttClient(modeMqttHost, modeMqttPort,
		WithMqttDelegate(goodDelegate))
	ctx, cancel := goodDelegate.createContext()
	defer cancel()
	client.Connect(ctx)

	cmdCh <- publishCommandCmd
	// Start the listener
	goodDelegate.StartSubscriptionListener()

	assert.True(t, client.IsConnected(), "Lost connection")
	assert.Nil(t, client.Subscribe(ctx), "failed to subscribe")

	receivedSubData := false
	select {
	case cmd := <-cmdChannel:
		fmt.Printf("Received the command from the server: %s\n", cmd.Action)
		receivedSubData = true
		break
	case <-ctx.Done():
		t.Errorf("Timed out waiting for command")
	}

	assert.True(t, receivedSubData, "Did not receive command")

	fmt.Println("Exiting")
	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cmdCh <- shutdownCmd
	wg.Wait()
}
