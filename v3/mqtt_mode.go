// Source for the MODE specific MQTT protocol.
package mode

import (
	"bytes"
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

		requestTimeout time.Duration

		// For handling our subscriptions. Output
		command chan<- *DeviceCommand
		kvSync  chan<- *KeyValueSync

		// For receiving data from the API. Input (but we create and manage
		// them)
		SubRecvCh  chan MqttSubData
		QueueAckCh chan MqttQueueResult
		PingAckCh  chan bool
	}
)

// Maybe have the channel sizes as parameters.
func NewModeMqttDelegate(dc *DeviceContext, cmdQueue chan<- *DeviceCommand,
	kvSyncQueue chan<- *KeyValueSync) *ModeMqttDelegate {
	del := &ModeMqttDelegate{
		dc:             dc,
		requestTimeout: 4 * time.Second,
		command:        cmdQueue,
		kvSync:         kvSyncQueue,
		SubRecvCh:      make(chan MqttSubData),
		QueueAckCh:     make(chan MqttQueueResult),
		PingAckCh:      make(chan bool),
	}
	subs := make(map[string]mqttMsgHandler)
	subs[fmt.Sprintf("/devices/%d/command", dc.DeviceID)] = del.handleCommandMsg
	subs[fmt.Sprintf("/devices/%d/kv", dc.DeviceID)] = del.handleKeyValueMsg

	del.subscriptions = subs
	return del
}

// Non-MqttDelegate method to listen on subscriptions and pass the interpreted
// data to the appropriate channel (kvSync or command)
func (del ModeMqttDelegate) RunSubscriptionListener() {

	for {
		select {
		case subData := <-del.SubRecvCh:
			subBytes := subData.data
			// Determine which callback to call based on the
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

func (del ModeMqttDelegate) AuthInfo() (username string, password string) {
	// format as decimal
	return strconv.FormatUint(del.dc.DeviceID, 10), del.dc.AuthToken
}

func (del ModeMqttDelegate) ReceiveChannels() (subRecvCh chan<- MqttSubData,
	pubAckCh chan<- MqttQueueResult,
	pingAckCh chan<- bool) {
	return del.SubRecvCh, del.QueueAckCh, del.PingAckCh
}

func (del ModeMqttDelegate) RequestTimeout() time.Duration {
	return del.requestTimeout
}

func (del ModeMqttDelegate) Subscriptions() []string {
	keys := make([]string, len(del.subscriptions))

	i := 0
	for k := range del.subscriptions {
		keys[i] = k
		i++
	}
	return keys
}

func (del ModeMqttDelegate) Close() {
	close(del.command)
	close(del.kvSync)
}

func (del ModeMqttDelegate) handleCommandMsg(data []byte) error {
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

func (del ModeMqttDelegate) handleKeyValueMsg(data []byte) error {
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
func (client *MqttClient) getModeDelegate() (*ModeMqttDelegate, error) {
	implDelegate, ok := client.delegate.(*ModeMqttDelegate)
	if !ok {
		return implDelegate, fmt.Errorf("MqttClient was not created with Mode Delegate")
	} else {
		return implDelegate, nil
	}
}

// Helper function to send DeviceEvent instances
func (client *MqttClient) PublishEvent(event DeviceEvent) (uint16, error) {
	modeDel, err := client.getModeDelegate()
	if err != nil {
		return 0, err
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	topic := fmt.Sprintf("/devices/%d/event", modeDel.dc.DeviceID)

	return client.Publish(event.Qos, topic, payload)
}

// Helper function to send DeviceEvent instances. This replaces both the
// sendBulkData and writeBulkData methods in the old API (since it does less
// than both, covering just the intersection of the other
func (client *MqttClient) PublishBulkData(bulkData DeviceBulkData) (uint16,
	error) {
	modeDel, err := client.getModeDelegate()
	if err != nil {
		return 0, err
	}

	topic := fmt.Sprintf("/devices/%d/bulkData/%s", modeDel.dc.DeviceID,
		bulkData.StreamID)

	return client.Publish(bulkData.Qos, topic, bulkData.Blob)
}

// The key value store should typically be cached. Key Values are all sent on
// subscription, so should be handled in the client by receiving on the kvSync
// channel. There is no method of fetching single key value pairs. We only
// update the key values.
// XXX: Looks like there's a reload, so I must be missing something.
func (client *MqttClient) PublishKeyValueUpdate(kvData KeyValueSync) (uint16,
	error) {
	modeDel, err := client.getModeDelegate()
	if err != nil {
		return 0, err
	}

	payload, err := json.Marshal(kvData)
	if err != nil {
		return 0, err
	}
	topic := fmt.Sprintf("/devices/%d/kv", modeDel.dc.DeviceID)

	// Hardcode QOS1
	return client.Publish(QOSAtLeastOnce, topic, payload)
}

// A special JSON decoder that makes sure numbers in command parameters
// are preserved (avoid turning integers into floats).
func decodeOpaqueJSON(b []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	return decoder.Decode(v)
}
