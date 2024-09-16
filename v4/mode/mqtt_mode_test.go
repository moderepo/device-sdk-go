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
	// mqtt dummy server
	modeMqttHost = "localhost"
	modeMqttPort = 1998
)

func invalidModeMqttDelegate() *ModeMqttDelegate {
	dc := &DeviceContext{
		DeviceID:  1,
		AuthToken: "XXXX",
	}

	return NewModeMqttDelegate(dc)
}

func newModeMqttDelegate() *ModeMqttDelegate {
	dc := &DeviceContext{
		DeviceID:  1234,
		AuthToken: "v1.XXXXXXXXX",
	}

	return NewModeMqttDelegate(dc, WithUseTLS(false))
}

// Helper for some tests
func testModeConnection(ctx context.Context, t *testing.T,
	delegate *ModeMqttDelegate, expectError bool) *MqttClient {
	client := NewMqttClient(modeMqttHost, modeMqttPort,
		WithMqttDelegate(delegate))
	ctx, cancel := context.WithCancel(ctx)
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

func TestModeMqttDelegate(t *testing.T) {
	dc := &DeviceContext{
		DeviceID:  1,
		AuthToken: "XXXX",
	}

	extraSub := "additionalsub"
	extraFormatSub := "additional/%d/sub"
	expectedSubs := []string{
		fmt.Sprintf("/devices/%d/command", dc.DeviceID),
		fmt.Sprintf("/devices/%d/kv", dc.DeviceID),
		extraSub,
		fmt.Sprintf(extraFormatSub, dc.DeviceID)}

	// Test the default and overridden values of the mode delegate
	t.Run("TestDefaultModeMqttDelegate", func(t *testing.T) {
		del := NewModeMqttDelegate(dc)

		useTLS := del.UseTLS()
		// Default is not to use TLS
		assert.Equal(t, defaultUseTLS, useTLS, "Use TLS expected to be true")
		user, password := del.AuthInfo()
		assert.Equal(t, fmt.Sprintf("%d", dc.DeviceID), user, "User did not match device ID")
		assert.Equal(t, dc.AuthToken, password, "Password did not match device auth token")
		assert.Equal(t, del.GetReceiveQueueSize(), defaultQueueSize,
			"Receive queue size did not match default")
		assert.Equal(t, del.GetSendQueueSize(), defaultQueueSize,
			"Receive queue size did not match default")

		// Just the first two subs of the expectedSubs will be found
		for i := 0; i < 2; i++ {
			sub := expectedSubs[i]
			_, found := del.subscriptions[sub]
			assert.True(t, found,
				"Should have found %s among the subscriptions", sub)
		}
	})

	t.Run("TestModeMqttDelegateOverrides", func(t *testing.T) {
		receiveQueueSize := uint16(1)
		sendQueueSize := uint16(2)

		del := NewModeMqttDelegate(dc,
			WithUseTLS(false),
			WithReceiveQueueSize(receiveQueueSize),
			WithSendQueueSize(sendQueueSize),
			WithAdditionalSubscription("additionalsub", nil),
			WithAdditionalFormatSubscription("additional/%d/sub", nil))

		useTLS := del.UseTLS()
		assert.False(t, useTLS, "Use TLS expected to be false")
		assert.Equal(t, del.GetReceiveQueueSize(), receiveQueueSize,
			"Receive queue size did not match set value")
		assert.Equal(t, del.GetSendQueueSize(), sendQueueSize,
			"Send queue size did not match set value")

		for _, sub := range expectedSubs {
			_, found := del.subscriptions[sub]
			assert.True(t, found,
				"Should have found %s among the subscriptions", sub)
		}
	})
}

func TestModeMqttClientConnection(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestModeMqttClientConnection: test bad user/pass")
	badDelegate := invalidModeMqttDelegate()
	testModeConnection(ctx, t, badDelegate, true)

	fmt.Println("TestModeMqttClientConnection: test good user/pass")
	goodDelegate := newModeMqttDelegate()
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
	DummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestModeMqttClientSubscribe")
	goodDelegate := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)
	errs := client.Subscribe(ctx, goodDelegate.Subscriptions())
	assert.Nil(t, errs, "failed to subscribe")

	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

func TestModeMqttClientPing(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientPing")
	goodDelegate := newModeMqttDelegate()
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
	DummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientPublishEvent")
	goodDelegate := newModeMqttDelegate()
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
// hit a bug with BulkData publishing, I think that's sufficient.)
func TestModeMqttClientPublishBulkData(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientPublishBulkData")
	goodDelegate := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)
	assert.True(t, client.IsConnected(), "Lost connection")

	bulk := DeviceBulkData{
		Label: "stream1",
		Blob:  []byte("blob"),
		Qos:   QOSAtLeastOnce,
	}
	packetID, err := client.PublishBulkData(ctx, bulk)
	assert.Nil(t, err, "PublishDeviceBulkData send failed")

	returnID := testModeWaitForPubAck(t, goodDelegate, false)
	assert.Equal(t, returnID, packetID, "Send packet did not match Ack packet")

	logInfo("[MQTT Test] disconnecting")
	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

func TestModeMqttClientPublishDataForRouting(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientPublishDataForRouting")
	goodDelegate := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)
	assert.True(t, client.IsConnected(), "Lost connection")

	packetID, err := client.PublishDataForRouting(ctx, QOSAtLeastOnce, []byte("blob"))
	assert.Nil(t, err, "PublishDataForRouting send failed")

	returnID := testModeWaitForPubAck(t, goodDelegate, false)
	assert.Equal(t, returnID, packetID, "Send packet did not match Ack packet")

	logInfo("[MQTT Test] disconnecting")
	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

func TestModeMqttClientSaveKeyValue(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientSaveKeyValue")
	goodDelegate := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)
	assert.True(t, client.IsConnected(), "Lost connection")

	// Set the value
	packetID, err := client.SaveKeyValue(ctx, "boo99", "far99")
	assert.Nil(t, err, "SaveKeyValue failed")

	returnID := testModeWaitForPubAck(t, goodDelegate, false)
	assert.Equal(t, returnID, packetID, "Send packet did not match Ack packet")

	logInfo("[MQTT Test] disconnecting")
	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

func TestModeMqttClientDeleteKeyValue(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientDeleteKeyValue")
	goodDelegate := newModeMqttDelegate()
	client := testModeConnection(ctx, t, goodDelegate, false)
	assert.True(t, client.IsConnected(), "Lost connection")

	// Delete the value
	packetID, err := client.DeleteKeyValue(ctx, "boo99")
	assert.Nil(t, err, "DeleteKeyValue failed")

	returnID := testModeWaitForPubAck(t, goodDelegate, true)
	assert.Equal(t, returnID, packetID, "Send packet did not match Ack packet")

	logInfo("[MQTT Test] disconnecting")
	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

// When we first connect and subscribe to the kv topic, we'll get a sync.
// This is a good test to see that we're receiving.
func TestModeMqttClientReceiveKVSync(t *testing.T) {
	var wg sync.WaitGroup
	cmdCh := make(chan DummyCmd)
	wg.Add(1)
	DummyMQTTD(nil, &wg, cmdCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("TestModeMqttClientReceiveKvSync")
	goodDelegate := newModeMqttDelegate()
	client := NewMqttClient(modeMqttHost, modeMqttPort,
		WithMqttDelegate(goodDelegate))
	err := client.Connect(ctx)
	assert.NoError(t, err)
	// Tell the server to send a kv update
	cmdCh <- PublishKvSync
	// Start the listener
	goodDelegate.StartSubscriptionListener()

	assert.True(t, client.IsConnected(), "Lost connection")
	assert.Nil(t, client.Subscribe(ctx, goodDelegate.Subscriptions()),
		"failed to subscribe")

	d := time.Now().Add(2 * time.Second)
	syncCtx, syncCancel := context.WithDeadline(context.Background(), d)
	defer syncCancel()

	receivedSubData := false
	select {
	case kvSync := <-goodDelegate.GetKVSyncChannel():
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

	cmdCh <- ShutdownCmd
	wg.Wait()
}

// We need to wait for someone to send this to us, so I'm not sure how to
// initiate this automatically
func TestModeMqttClientReceiveCommand(t *testing.T) {
	var wg sync.WaitGroup
	cmdCh := make(chan DummyCmd)
	wg.Add(1)
	DummyMQTTD(nil, &wg, cmdCh)

	fmt.Println("TestModeMqttClientReceiveCommand")
	goodDelegate := newModeMqttDelegate()
	client := NewMqttClient(modeMqttHost, modeMqttPort,
		WithMqttDelegate(goodDelegate))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := client.Connect(ctx)
	assert.NoError(t, err)

	cmdCh <- PublishCommandCmd
	// Start the listener
	goodDelegate.StartSubscriptionListener()

	assert.True(t, client.IsConnected(), "Lost connection")
	assert.Nil(t, client.Subscribe(ctx, goodDelegate.Subscriptions()),
		"failed to subscribe")

	receivedSubData := false
	select {
	case cmd := <-goodDelegate.GetCommandChannel():
		fmt.Printf("Received the command from the server: %s\n", cmd.Action)
		receivedSubData = true
		break
	case <-ctx.Done():
		t.Errorf("Timed out waiting for command")
	}

	assert.True(t, receivedSubData, "Did not receive command")

	fmt.Println("Exiting")
	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cmdCh <- ShutdownCmd
	wg.Wait()
}
