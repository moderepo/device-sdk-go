// Source for the MODE specific MQTT protocol.
package mode

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const (
	KVSyncActionReload = "reload"
	KVSyncActionSet    = "set"
	KVSyncActionDelete = "delete"
)

type (
	mqttMsgHandler func([]byte) error

	// DeviceCommand represents a received from the MODE cloud.
	DeviceCommand struct {
		Action  string
		payload json.RawMessage
	}

	KeyValueSync struct {
		Action   string      `json:"action"`
		Revision int         `json:"rev"`
		Key      string      `json:"key"`
		Value    interface{} `json:"value"`
		NumItems int         `json:"numItems"`
		Items    []*KeyValue `json:"items"`
	}

	// KeyValue represents a key-value pair stored in the Device Data Proxy.
	KeyValue struct {
		Key              string      `json:"key"`
		Value            interface{} `json:"value"`
		ModificationTime time.Time   `json:"modificationTime"`
	}

	// DeviceEvent represents an event to be sent to the MODE cloud.
	DeviceEvent struct {
		EventType string                 `json:"eventType"`
		EventData map[string]interface{} `json:"eventData,omitempty"`
		Qos       QOSLevel               // not exported to JSON
	}

	// BulkData represents a batch of opaque data to be sent to the MODE cloud.
	DeviceBulkData struct {
		StreamID string
		Blob     []byte
		Qos      QOSLevel // not exported to serializer
	}

	// Implements the MqttDelegate
	ModeMqttDelegate struct {
		dc            *DeviceContext
		subscriptions map[string]mqttMsgHandler

		receiveQueueSize uint16
		sendQueueSize    uint16
		responseTimeout  time.Duration

		// For handling our subscriptions. Output
		command chan<- *DeviceCommand
		kvSync  chan<- *KeyValueSync

		// For receiving data from the API. Input
		SubRecvCh  <-chan MqttSubData
		QueueAckCh <-chan MqttResponse
		PingAckCh  <-chan MqttResponse
	}
)

type (
	ModeMqttOptDuration    time.Duration
	ModeMqttOptQueueLength int16

	ModeMqttDelegateOpt interface {
		apply(d *ModeMqttDelegate)
	}
)

func (dur ModeMqttOptDuration) apply(del *ModeMqttDelegate) {
	del.responseTimeout = time.Duration(dur)
}

func (len ModeMqttOptQueueLength) apply(del *ModeMqttDelegate) {
	del.sendQueueSize = uint16(len)
}

// Maybe have the channel sizes as parameters.
func NewModeMqttDelegate(
	dc *DeviceContext,
	cmdQueue chan<- *DeviceCommand,
	kvSyncQueue chan<- *KeyValueSync,
	opts ...ModeMqttDelegateOpt) *ModeMqttDelegate {
	del := &ModeMqttDelegate{
		dc:               dc,
		receiveQueueSize: 8,               // some default
		sendQueueSize:    8,               // some default
		responseTimeout:  2 * time.Second, // some default
		command:          cmdQueue,
		kvSync:           kvSyncQueue,
		SubRecvCh:        make(chan MqttSubData),
		QueueAckCh:       make(chan MqttResponse),
		PingAckCh:        make(chan MqttResponse),
	}
	subs := make(map[string]mqttMsgHandler)
	subs[fmt.Sprintf("/devices/%d/command", dc.DeviceID)] = del.handleCommandMsg
	subs[fmt.Sprintf("/devices/%d/kv", dc.DeviceID)] = del.handleKeyValueMsg

	del.subscriptions = subs

	for _, opt := range opts {
		opt.apply(del)
	}
	return del
}

func (del *ModeMqttDelegate) GetDeviceContext() *DeviceContext {
	return del.dc
}

// Non-MqttDelegate method to listen on subscriptions and pass the interpreted
// data to the appropriate channel (kvSync or command)
func (del *ModeMqttDelegate) RunSubscriptionListener() {

	for {
		select {
		case subData := <-del.SubRecvCh:
			subBytes := subData.data
			// Determine which callback to call based on the topic
			if handler, exists := del.subscriptions[subData.topic]; exists {
				if err := handler(subBytes); err != nil {
					logError("Error in subscription handler: %s", err)
				}
			} else {
				logError("No subscription handler for %s", subData.topic)
			}
		}
	}
}

func (del *ModeMqttDelegate) TLSUsageAndConfiguration() (useTLS bool,
	tlsConfig *tls.Config) {
	// useTLS is a package level variable
	return useTLS, del.dc.TLSConfig
}

func (del *ModeMqttDelegate) AuthInfo() (username string, password string) {
	// format as decimal
	return strconv.FormatUint(del.dc.DeviceID, 10), del.dc.AuthToken
}

func (del *ModeMqttDelegate) SetReceiveChannels(subRecvCh <-chan MqttSubData,
	queueAckCh <-chan MqttResponse,
	pingAckCh <-chan MqttResponse) {
	del.SubRecvCh = subRecvCh
	del.QueueAckCh = queueAckCh
	del.PingAckCh = pingAckCh
}

func (del *ModeMqttDelegate) GetReceiveQueueSize() uint16 {
	return del.receiveQueueSize
}

func (del *ModeMqttDelegate) GetSendQueueSize() uint16 {
	return del.sendQueueSize
}

func (del *ModeMqttDelegate) GetSubscriptions() []string {
	keys := make([]string, len(del.subscriptions))

	i := 0
	for k := range del.subscriptions {
		keys[i] = k
		i++
	}
	return keys
}

func (del *ModeMqttDelegate) OnClose() {
	close(del.command)
	close(del.kvSync)
}

func (del *ModeMqttDelegate) createContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), del.responseTimeout)
}

func (del *ModeMqttDelegate) handleCommandMsg(data []byte) error {
	var cmd struct {
		Action     string          `json:"action"`
		Parameters json.RawMessage `json:"parameters"`
	}

	if err := decodeOpaqueJSON(data, &cmd); err != nil {
		return fmt.Errorf("message data is not valid command JSON: %s",
			err.Error())
	}

	if cmd.Action == "" {
		return errors.New("message data is not valid command JSON: no action field")
	}

	del.command <- &DeviceCommand{Action: cmd.Action, payload: cmd.Parameters}
	return nil
}

func (del *ModeMqttDelegate) handleKeyValueMsg(data []byte) error {
	var kvSync KeyValueSync

	if err := decodeOpaqueJSON(data, &kvSync); err != nil {
		return fmt.Errorf("message data is not valid key-value sync JSON: %s",
			err.Error())
	}

	if kvSync.Action == "" {
		return errors.New("message data is not valid key-value sync JSON: no action field")
	}

	fmt.Println("Sending to kvSync channel")
	del.kvSync <- &kvSync
	return nil
}

// Mode extensions to the MqttClient
// cast to the concrete delegate
func (client *MqttClient) GetModeDelegate() (*ModeMqttDelegate, error) {
	implDelegate, ok := client.delegate.(*ModeMqttDelegate)
	if !ok {
		return implDelegate, fmt.Errorf("MqttClient was not created with Mode Delegate")
	}
	return implDelegate, nil
}

// Helper function to send DeviceEvent instances
func (client *MqttClient) PublishEvent(event DeviceEvent) (uint16, error) {
	modeDel, err := client.GetModeDelegate()
	if err != nil {
		return 0, err
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	topic := fmt.Sprintf("/devices/%d/event", modeDel.dc.DeviceID)
	ctx, cancel := modeDel.createContext()
	defer cancel()
	return client.Publish(ctx, event.Qos, topic, payload)
}

// Helper function to send DeviceEvent instances. This replaces both the
// sendBulkData and writeBulkData methods in the old API (since it does less
// than both, covering just the intersection)
func (client *MqttClient) PublishBulkData(bulkData DeviceBulkData) (uint16,
	error) {
	modeDel, err := client.GetModeDelegate()
	if err != nil {
		return 0, err
	}

	topic := fmt.Sprintf("/devices/%d/bulkData/%s", modeDel.dc.DeviceID,
		bulkData.StreamID)

	ctx, cancel := modeDel.createContext()
	defer cancel()
	return client.Publish(ctx, bulkData.Qos, topic, bulkData.Blob)
}

// The key value store should typically be cached. Key Values are all sent on
// subscription, so should be handled in the client by receiving on the kvSync
// channel. There is no method of fetching single key value pairs. We only
// update the key values.
// XXX: Looks like there's a reload, so I must be missing something.
func (client *MqttClient) PublishKeyValueUpdate(kvData KeyValueSync) (uint16,
	error) {
	modeDel, err := client.GetModeDelegate()
	if err != nil {
		return 0, err
	}

	payload, err := json.Marshal(kvData)
	if err != nil {
		return 0, err
	}
	topic := fmt.Sprintf("/devices/%d/kv", modeDel.dc.DeviceID)

	// Hardcode QOS1
	ctx, cancel := modeDel.createContext()
	defer cancel()
	return client.Publish(ctx, QOSAtLeastOnce, topic, payload)
}

// A special JSON decoder that makes sure numbers in command parameters
// are preserved (avoid turning integers into floats).
func decodeOpaqueJSON(b []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	return decoder.Decode(v)
}
