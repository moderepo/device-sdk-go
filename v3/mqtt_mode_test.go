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

		usage, conf := del.TLSUsageAndConfiguration()
		// Default is not to use TLS
		assert.Equal(t, DefaultUseTLS, usage, "Use TLS expected to be true")
		assert.Nil(t, conf, "Expected no TLS config")
		user, password := del.AuthInfo()
		assert.Equal(t, fmt.Sprintf("%d", dc.DeviceID), user, "User did not match device ID")
		assert.Equal(t, dc.AuthToken, password, "Password did not match device auth token")
		assert.Equal(t, del.GetReceiveQueueSize(), DefaultQueueSize,
			"Receive queue size did not match default")
		assert.Equal(t, del.GetSendQueueSize(), DefaultQueueSize,
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
		skipVerify := false
		err := dc.SetPKCS12ClientCertificate("fixtures/client1.p12", "pwd", skipVerify)
		assert.Nil(t, err, "Failed to load certificate")
		dc.TLSClientAuth = true

		receiveQueueSize := uint16(1)
		sendQueueSize := uint16(2)

		del := NewModeMqttDelegate(dc,
			WithUseTLS(false),
			WithReceiveQueueSize(receiveQueueSize),
			WithSendQueueSize(sendQueueSize),
			WithAdditionalSubscription("additionalsub", nil),
			WithAdditionalFormatSubscription("additional/%d/sub", nil))

		usage, conf := del.TLSUsageAndConfiguration()
		assert.False(t, usage, "Use TLS expected to be false")
		assert.Equal(t, dc.TLSConfig, conf, "Mismatch in TLSConfg")
		_, password := del.AuthInfo()
		assert.Empty(t, password,
			"Should have empty password if TLSClientAuth is true")
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
// hit a bug with BulkData publishing, I think that's sufficient.
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
	DummyMQTTD(ctx, &wg, nil)

	fmt.Println("TestMqttClientPublishKVUpdate")
	goodDelegate := newModeMqttDelegate()
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
	cmdCh := make(chan DummyCmd)
	wg.Add(1)
	DummyMQTTD(nil, &wg, cmdCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("TestModeMqttClientReceiveKvSync")
	goodDelegate := newModeMqttDelegate()
	client := NewMqttClient(modeMqttHost, modeMqttPort,
		WithMqttDelegate(goodDelegate))
	client.Connect(ctx)
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
	client.Connect(ctx)

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
