package mode

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type (
	TestMqttDelegate struct {
		username         string
		password         string
		subscriptions    []string
		receiveQueueSize uint16
		sendQueueSize    uint16
		responseTimeout  time.Duration
		subRecvCh        <-chan MqttSubData
		queueAckCh       <-chan MqttResponse
		pingAckCh        <-chan MqttResponse
	}

	// Testing with single responsibility delegates
	TestMqttAuthDelegate struct {
	}
	TestMqttReceiverDelegate struct {
	}
	TestMqttConfigDelegate struct {
	}

	TestMqttErrorDelegate struct {
		errorChBufSize uint16
		errorCh        chan error
	}
)

func (*TestMqttAuthDelegate) TLSUsageAndConfiguration() (useTLS bool, tlsConfig *tls.Config) {
	return true, nil
}

func (*TestMqttAuthDelegate) AuthInfo() (username string, password string) {
	return "foo", "bar"
}

func (*TestMqttReceiverDelegate) SetReceiveChannels(_ <-chan MqttSubData,
	_ <-chan MqttResponse,
	_ <-chan MqttResponse) {
	// Just ignore everything for the test
}

func (*TestMqttReceiverDelegate) OnClose() {
}

func (*TestMqttConfigDelegate) GetReceiveQueueSize() uint16 {
	return uint16(0)
}

func (*TestMqttConfigDelegate) GetSendQueueSize() uint16 {
	return uint16(0)
}

func (ed *TestMqttErrorDelegate) GetErrorChannelSize() uint16 {
	return ed.errorChBufSize
}

func (ed *TestMqttErrorDelegate) SetErrorChannel(errCh chan error) {
	ed.errorCh = errCh
}

func newTestMqttDelegate() *TestMqttDelegate {
	d := &TestMqttDelegate{
		username:         "good",
		password:         "anything",
		receiveQueueSize: 2,
		sendQueueSize:    2,
		responseTimeout:  2 * time.Second,
		subscriptions: []string{
			fmt.Sprintf("/devices/%s/command", "foo"),
			fmt.Sprintf("/devices/%s/kv", "bar"),
		},
	}

	return d
}

func newTestMqttDelegateWithSettings(recvSize uint16,
	sendSize uint16) *TestMqttDelegate {
	d := &TestMqttDelegate{
		username:         "good",
		password:         "anything",
		receiveQueueSize: recvSize,
		sendQueueSize:    sendSize,
		responseTimeout:  2 * time.Second,
		subscriptions: []string{
			fmt.Sprintf("/devices/%s/command", "foo"),
			fmt.Sprintf("/devices/%s/kv", "bar"),
		},
	}

	return d
}

func invalidTestMqttDelegate() TestMqttDelegate {
	return TestMqttDelegate{
		username:         "foo",
		password:         "bar",
		receiveQueueSize: 2,
		sendQueueSize:    2,
		responseTimeout:  2 * time.Second,
	}
}

func (del *TestMqttDelegate) TLSUsageAndConfiguration() (useTLS bool,
	tlsConfig *tls.Config) {
	return useTLS, nil
}

func (del *TestMqttDelegate) AuthInfo() (username string, password string) {
	return del.username, del.password
}

func (del *TestMqttDelegate) SetReceiveChannels(subRecvCh <-chan MqttSubData,
	queueAckCh <-chan MqttResponse,
	pingAckCh <-chan MqttResponse) {
	del.subRecvCh = subRecvCh
	del.queueAckCh = queueAckCh
	del.pingAckCh = pingAckCh
}

func (del *TestMqttDelegate) GetReceiveQueueSize() uint16 {
	return del.receiveQueueSize
}

func (del *TestMqttDelegate) GetSendQueueSize() uint16 {
	return del.sendQueueSize
}

func (del *TestMqttDelegate) OnClose() {
}

func (del *TestMqttDelegate) createContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), del.responseTimeout)
}

func testConnection(ctx context.Context, t *testing.T, delegate MqttDelegate,
	expectError bool) *MqttClient {

	client := NewMqttClient(DefaultMqttHost, DefaultMqttPort,
		WithMqttDelegate(delegate))
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

func TestCreateMqttClient(t *testing.T) {
	// Create all the delegate types: auth, recv, conf, and uber
	allInOne := &TestMqttDelegate{}
	authDel := &TestMqttAuthDelegate{}
	recvDel := &TestMqttReceiverDelegate{}
	confDel := &TestMqttConfigDelegate{}

	t.Run("Incomplete implementations", func(t *testing.T) {
		var client *MqttClient
		client = NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttAuthDelegate(authDel))
		assert.Nil(t, client, "Incomplete delegate implementation succeeded")
		client = NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttReceiverDelegate(recvDel))
		assert.Nil(t, client, "Incomplete delegate implementation succeeded")
		client = NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttConfigDelegate(confDel))
		assert.Nil(t, client, "Incomplete delegate implementation succeeded")
		client = NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttAuthDelegate(authDel),
			WithMqttConfigDelegate(confDel))
		assert.Nil(t, client, "Incomplete delegate implementation succeeded")
	})

	t.Run("Good combinations", func(t *testing.T) {
		client := NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttDelegate(allInOne))
		assert.NotNil(t, client, "Unexpected failure in creating client")
		client = NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttAuthDelegate(authDel),
			WithMqttReceiverDelegate(recvDel),
			WithMqttConfigDelegate(confDel))
		assert.NotNil(t, client, "Unexpected failure in creating client")
	})
}

func TestMqttClientConnection(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)

	t.Run("Fail to Connect bad user/pass", func(t *testing.T) {
		badDelegate := invalidTestMqttDelegate()
		testConnection(ctx, t, &badDelegate, true)
	})
	t.Run("Connect good user/pass", func(t *testing.T) {
		goodDelegate := newTestMqttDelegate()
		client := testConnection(ctx, t, goodDelegate, false)
		assert.Nil(t, client.Disconnect(ctx), "error disconnecting")
	})

	cancel()
	wg.Wait()
}

func TestMqttClientReconnect(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)
	delegate := newTestMqttDelegate()

	t.Run("Connect", func(t *testing.T) {
		client := NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttDelegate(delegate))
		err := client.Connect(ctx)
		assert.Nil(t, err, "Failed to connect to client")
		// Reconnect before disconnect:
		err = client.Connect(ctx)
		assert.NotNil(t, err, "Connect should fail if not Disconnected")
		err = client.Disconnect(ctx)
		assert.Nil(t, err, "Failed to disconnect from client")
	})
	t.Run("ReConnect", func(t *testing.T) {
		// recreate the closed channels that were closed on OnClose()
		client := NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttDelegate(delegate))
		err := client.Connect(ctx)
		assert.Nil(t, err, "Failed to connect to client")
		err = client.Disconnect(ctx)
		assert.Nil(t, err, "Failed to disconnect from client")
	})

	cancel()
	wg.Wait()
}

func TestMqttClientSubscribe(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)
	fmt.Println("TestMqttClientSubscribe")
	goodDelegate := newTestMqttDelegate()
	client := testConnection(ctx, t, goodDelegate, false)
	if errs := client.Subscribe(ctx, goodDelegate.subscriptions); errs != nil {
		t.Errorf("failed to subscribe: %s", errs)
	}

	if client.Disconnect(ctx) != nil {
		t.Errorf("error disconnecting")
	}
	cancel()
	wg.Wait()
}

func TestMqttClientUnSubscribe(t *testing.T) {
	// We only make sure we can send this and get a successful unsubscribe
	// back.
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)
	fmt.Println("TestMqttClientUnsubscribe")
	goodDelegate := newTestMqttDelegate()
	client := testConnection(ctx, t, goodDelegate, false)
	assert.Nil(t, client.Unsubscribe(ctx, goodDelegate.subscriptions),
		"Failed to unsubscribe")
	assert.Nil(t, client.Disconnect(ctx), "error disconnecting")

	cancel()
	wg.Wait()
}

func TestMqttClientPublish(t *testing.T) {
	var wg sync.WaitGroup
	delegate := newTestMqttDelegate()
	ctx, cancel := delegate.createContext()
	wg.Add(1)
	DummyMQTTD(ctx, &wg, nil)
	fmt.Println("TestMqttClientPublish")

	t.Run("Test Publish With ACK", func(t *testing.T) {
		client := testConnection(ctx, t, delegate, false)
		topic := fmt.Sprintf("/devices/%s/event", delegate.username)
		event := DeviceEvent{
			EventType: "CustomEvent",
			EventData: map[string]interface{}{"key1": "val1"},
			Qos:       QOSAtLeastOnce,
		}
		rawData, err := json.Marshal(event)
		assert.Nil(t, err, "JSON marshaling failed")
		packetID, err := client.Publish(ctx, event.Qos, topic, rawData)
		assert.Nil(t, err, "Publish send failed")

		// Wait for the ack.
		select {
		case ackData := <-delegate.queueAckCh:
			assert.Nil(t, ackData.Err, "Publish ack failed")
			assert.Equal(t, packetID, ackData.PacketID,
				"Publish: ID and ack id do not match")
		case <-ctx.Done():
			t.Errorf("Publish response never received")
		}
		err = client.Disconnect(ctx)
		assert.Nil(t, err, "error disconnecting")
	})

	t.Run("Test Republish (ignore ACK)", func(t *testing.T) {
		client := testConnection(ctx, t, delegate, false)
		topic := fmt.Sprintf("/devices/%s/event", delegate.username)
		event := DeviceEvent{
			EventType: "CustomEvent",
			EventData: map[string]interface{}{"key1": "val1"},
			Qos:       QOSAtLeastOnce,
		}
		rawData, err := json.Marshal(event)
		assert.Nil(t, err, "JSON marshaling failed")
		packetID, err := client.Publish(ctx, event.Qos, topic, rawData)
		assert.Nil(t, err, "Publish send failed")
		repubPacketID, err := client.Republish(ctx, event.Qos, topic, rawData,
			packetID)
		assert.Nil(t, err, "RePublish send failed")
		assert.Equal(t, packetID, repubPacketID,
			"Publish and Republish had different packet IDs")
		err = client.Disconnect(ctx)
		assert.Nil(t, err, "error disconnecting")
	})
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
		logInfo("sendPing received ping response")
		return resp.Err
	case <-ctx.Done():
		// If we're testing timeouts, this is expected. Not always test
		// failure
		return fmt.Errorf("Timed out waiting for ping response")
	}
}

func TestMqttClientPing(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	cmdCh := make(chan DummyCmd)
	DummyMQTTD(nil, &wg, cmdCh)

	delegate := newTestMqttDelegate()

	t.Run("Ping with No Connection", func(t *testing.T) {
		client := NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttDelegate(delegate))
		ctx, cancel := delegate.createContext()
		defer cancel()
		err := client.Ping(ctx)
		assert.Error(t, err, "Expected error in ping when not connected")
		err = client.PingAndWait(ctx)
		assert.Error(t, err, "Expected error in ping when not connected")
	})

	t.Run("Successful Async Ping", func(t *testing.T) {
		ctx, cancel := delegate.createContext()
		defer cancel()
		client := testConnection(ctx, t, delegate, false)
		err := sendPing(ctx, t, client, delegate)
		assert.Nil(t, err, "ping failed")
		err = client.Disconnect(ctx)
		assert.Nil(t, err, "error disconnecting")
	})

	t.Run("Successful sync Ping", func(t *testing.T) {
		ctx, cancel := delegate.createContext()
		defer cancel()
		client := testConnection(ctx, t, delegate, false)
		err := client.PingAndWait(ctx)
		assert.Nil(t, err, "ping failed")
		err = client.Disconnect(ctx)
		assert.Nil(t, err, "error disconnecting")
	})

	t.Run("Ping with Timeout", func(t *testing.T) {
		ctx, cancel := delegate.createContext()
		defer cancel()
		client := testConnection(ctx, t, delegate, false)
		cmdCh <- SlowdownServerCmd
		// Wait for the ack, but it will timeout
		err := sendPing(ctx, t, client, delegate)
		assert.Error(t, err, "Received expected error")
		// XXX - investigate why the test server is still slow. For now, ignore
		// the possible error on disconnect.
		client.Disconnect(ctx)

	})

	logInfo("Sending shutdown to server")
	cmdCh <- ShutdownCmd
	wg.Wait()
}

func waitForCondition(ctx context.Context, timeout time.Duration,
	cond func() bool) bool {
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, timeout)
	defer timeoutCancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	for !cond() {
		select {
		case <-ticker.C:
		case <-timeoutCtx.Done():
			return false
		}
	}
	return true
}

func TestMqttErrorHandling(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	cmdCh := make(chan DummyCmd)
	DummyMQTTD(nil, &wg, cmdCh)
	timeout := 4 * time.Second

	queueSize := uint16(2)
	delegate := newTestMqttDelegateWithSettings(queueSize, queueSize)

	t.Run("TestNoErrorDelegate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttDelegate(delegate))
		assert.Equal(t, len(client.TakeRemainingErrors()), 0,
			"Unexpected errors in last errors")
		assert.Nil(t, client.Connect(ctx), "Failed to connect")
		// Send some pings, but don't read the responses
		for i := 0; i < int(queueSize); i++ {
			assert.Nil(t, client.Ping(ctx), "Send of ping failed")
		}
		assert.True(t, waitForCondition(ctx, timeout, func() bool {
			if err := client.Ping(ctx); err != nil {
				return false
			}
			return len(delegate.pingAckCh) == 2
		}), "Ping response queue never filled")

		assert.Nil(t, client.Ping(ctx), "Send of ping failed")
		assert.True(t, waitForCondition(ctx, timeout, func() bool {
			return len(client.TakeRemainingErrors()) > 0
		}), "TakeRemainingErrors never populated")
		assert.Nil(t, client.Disconnect(ctx), "Error in disconnect")
	})

	t.Run("TestErrorDelegate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errorDelegate := TestMqttErrorDelegate{errorChBufSize: queueSize}
		client := NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttDelegate(delegate), WithMqttErrorDelegate(&errorDelegate))
		assert.Nil(t, client.Connect(ctx), "Failed to connect")
		// check that our error channel is empty
		assert.Equal(t, len(errorDelegate.errorCh), 0,
			"UnExpected errors in channela")
		// Send some pings, but don't read the responses
		for i := 0; i < int(queueSize); i++ {
			assert.Nil(t, client.Ping(ctx), "Send of ping failed")
		}
		assert.True(t, waitForCondition(ctx, timeout, func() bool {
			if err := client.Ping(ctx); err != nil {
				return false
			}
			return len(delegate.pingAckCh) == 2
		}), "Ping response queue never filled")

		assert.Nil(t, client.Ping(ctx), "Send of ping failed")
		assert.True(t, waitForCondition(ctx, timeout, func() bool {
			if err := client.Ping(ctx); err != nil {
				return false
			}
			return len(errorDelegate.errorCh) > 0
		}), "Error delegate  queue never filled")
		assert.Nil(t, client.Disconnect(ctx), "Error in disconnect")
	})

	t.Run("TestErrorDelegateOverflow", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errorDelegate := TestMqttErrorDelegate{errorChBufSize: queueSize}
		client := NewMqttClient(DefaultMqttHost, DefaultMqttPort,
			WithMqttDelegate(delegate), WithMqttErrorDelegate(&errorDelegate))
		assert.Nil(t, client.Connect(ctx), "Failed to connect")
		// check that our error channel is empty
		assert.Equal(t, len(errorDelegate.errorCh), 0,
			"Unexpected errors in channels")
		assert.Equal(t, len(client.TakeRemainingErrors()), 0,
			"Unexpected errors in last errors")

		// Fill two queues which are the same size, plus one overflow
		for i := 0; i <= 2*int(queueSize); i++ {
			assert.Nil(t, client.Ping(ctx), "Send of ping failed")
		}
		assert.True(t, waitForCondition(ctx, timeout, func() bool {
			if err := client.Ping(ctx); err != nil {
				return false
			}
			return len(delegate.pingAckCh) == 2
		}), "Ping response queue never filled")
		assert.True(t, waitForCondition(ctx, timeout, func() bool {
			if err := client.Ping(ctx); err != nil {
				return false
			}
			return len(errorDelegate.errorCh) == 2
		}), "Error delegate queue never filled")

		assert.True(t, waitForCondition(ctx, timeout, func() bool {
			if err := client.Ping(ctx); err != nil {
				return false
			}
			return len(client.TakeRemainingErrors()) > 0
		}), "TakeRemainingErrors never populated")

		assert.Nil(t, client.Disconnect(ctx), "Error in disconnect")
	})

	logInfo("Sending shutdown to server")
	cmdCh <- ShutdownCmd
	wg.Wait()
}
