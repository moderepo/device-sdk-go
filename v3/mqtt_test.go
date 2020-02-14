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

	client := NewMqttClient(testMqttHost, testMqttPort,
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
		client = NewMqttClient(testMqttHost, testMqttPort,
			WithMqttAuthDelegate(authDel))
		assert.Nil(t, client, "Incomplete delegate implementation succeeded")
		client = NewMqttClient(testMqttHost, testMqttPort,
			WithMqttReceiverDelegate(recvDel))
		assert.Nil(t, client, "Incomplete delegate implementation succeeded")
		client = NewMqttClient(testMqttHost, testMqttPort,
			WithMqttConfigDelegate(confDel))
		assert.Nil(t, client, "Incomplete delegate implementation succeeded")
		client = NewMqttClient(testMqttHost, testMqttPort,
			WithMqttAuthDelegate(authDel),
			WithMqttConfigDelegate(confDel))
		assert.Nil(t, client, "Incomplete delegate implementation succeeded")
	})

	t.Run("Good combinations", func(t *testing.T) {
		client := NewMqttClient(testMqttHost, testMqttPort,
			WithMqttDelegate(allInOne))
		assert.NotNil(t, client, "Unexpected failure in creating client")
		client = NewMqttClient(testMqttHost, testMqttPort,
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
	dummyMQTTD(ctx, &wg, nil)

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
	dummyMQTTD(ctx, &wg, nil)
	delegate := newTestMqttDelegate()

	t.Run("Connect", func(t *testing.T) {
		client := NewMqttClient(testMqttHost, testMqttPort,
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
		client := NewMqttClient(testMqttHost, testMqttPort,
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
	dummyMQTTD(ctx, &wg, nil)
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

func TestMqttClientPublish(t *testing.T) {
	var wg sync.WaitGroup
	delegate := newTestMqttDelegate()
	ctx, cancel := delegate.createContext()
	wg.Add(1)
	dummyMQTTD(ctx, &wg, nil)
	fmt.Println("TestMqttClientPublish")

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
		err = client.Disconnect(ctx)
		assert.Nil(t, err, "error disconnecting")
	case <-ctx.Done():
		t.Errorf("Publish response never received")
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
	cmdCh := make(chan dummyCmd)
	dummyMQTTD(nil, &wg, cmdCh)

	delegate := newTestMqttDelegate()

	t.Run("Successful Ping", func(t *testing.T) {
		ctx, cancel := delegate.createContext()
		defer cancel()
		client := testConnection(ctx, t, delegate, false)
		err := sendPing(ctx, t, client, delegate)
		assert.Nil(t, err, "ping failed")
		err = client.Disconnect(ctx)
		assert.Nil(t, err, "error disconnecting")
	})

	t.Run("Ping with Timeout", func(t *testing.T) {
		ctx, cancel := delegate.createContext()
		defer cancel()
		client := testConnection(ctx, t, delegate, false)
		cmdCh <- slowdownServerCmd
		// Wait for the ack, but it will timeout
		err := sendPing(ctx, t, client, delegate)
		assert.NotNil(t, err, "Received expected error")
		cmdCh <- resetServerCmd
		client.Disconnect(ctx)
	})

	t.Run("Ping with full queue", func(t *testing.T) {
		// Create a new delegate which has a bigger ping response buffer
		pingQueueSize := uint16(4)
		queueDel := &TestMqttDelegate{
			username:         "good",
			password:         "anything",
			receiveQueueSize: pingQueueSize,
			sendQueueSize:    2,
			// This test takes some round trips, so don't time out too soon
			responseTimeout: 5 * time.Second,
		}
		ctx, cancel := queueDel.createContext()
		defer cancel()
		client := testConnection(ctx, t, queueDel, false)
		assert.NotNil(t, client, "Failed to create a client and connect")

		// Loop through and ping 6 times.
		for i := uint16(0); i < pingQueueSize; i++ {
			logInfo("Sending ping: %d", i)
			assert.Nil(t, client.Ping(ctx), "Send of ping failed")
		}

		// Wait for the round trip, but don't check...
		timer := time.NewTimer(4 * time.Second)
		<-timer.C

		extraPings := int(2 * pingQueueSize)
		numPings := 0
		assert.Nil(t, client.GetLastError(), "Queue Already full?")
		var err error = nil
		// Since this is a synchronous, keep writing
		for err == nil && numPings < extraPings {
			assert.Nil(t, client.Ping(ctx), "Send of ping failed")
			timer = time.NewTimer(100 * time.Millisecond)
			<-timer.C
			err = client.GetLastError()
		}
		assert.NotNil(t, err, "Should have hit resonse queue error")

		err = client.Disconnect(ctx)
		assert.Nil(t, err, "error disconnecting")
	})

	logInfo("Sending shutdown to server")
	cmdCh <- shutdownCmd
	wg.Wait()
}
