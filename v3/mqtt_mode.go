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
	"strings"
	"time"
)

const (
	DefaultUseTLS    = true
	DefaultQueueSize = uint16(8)
)

const (
	KVSyncActionReload = "reload"
	KVSyncActionSet    = "set"
	KVSyncActionDelete = "delete"
)

type (
	MqttMsgHandler func([]byte) error

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
		subscriptions map[string]MqttMsgHandler
		UseTLS        bool

		receiveQueueSize uint16
		sendQueueSize    uint16

		// For handling our subscriptions. Output
		command chan *DeviceCommand
		kvSync  chan *KeyValueSync

		// Stop listening for incoming subscription data
		stopSubCh chan bool
		// For receiving data from the API. Input
		SubRecvCh  <-chan MqttSubData
		QueueAckCh <-chan MqttResponse
		PingAckCh  <-chan MqttResponse
	}

	ModeMqttDelegateOption func(*ModeMqttDelegate)
)

var _ MqttDelegate = (*ModeMqttDelegate)(nil)

func WithUseTLS(useTLS bool) func(*ModeMqttDelegate) {
	return func(d *ModeMqttDelegate) {
		d.UseTLS = useTLS
	}
}

func WithReceiveQueueSize(qSize uint16) func(*ModeMqttDelegate) {
	return func(d *ModeMqttDelegate) {
		d.receiveQueueSize = qSize
	}
}

func WithSendQueueSize(qSize uint16) func(*ModeMqttDelegate) {
	return func(d *ModeMqttDelegate) {
		d.sendQueueSize = qSize
	}
}

func WithAdditionalSubscription(topic string,
	handler MqttMsgHandler) func(*ModeMqttDelegate) {
	return func(d *ModeMqttDelegate) {
		d.subscriptions[topic] = handler
	}
}

// This is a little obtuse, but allows a format string where we substitute
// a %d. So, we check for the %d in the string.
func WithAdditionalFormatSubscription(formatTopic string,
	handler MqttMsgHandler) func(*ModeMqttDelegate) {
	// panic if this is not the correct format.
	if strings.Index(formatTopic, "%d") == -1 {
		panic("No %d in topic's format string")
	}
	return func(d *ModeMqttDelegate) {
		d.subscriptions[fmt.Sprintf(formatTopic, d.dc.DeviceID)] = handler
	}
}

// Maybe have the channel sizes as parameters.
func NewModeMqttDelegate(dc *DeviceContext,
	opts ...ModeMqttDelegateOption) *ModeMqttDelegate {
	del := &ModeMqttDelegate{
		dc:               dc,
		UseTLS:           DefaultUseTLS,
		receiveQueueSize: DefaultQueueSize, // some default
		sendQueueSize:    DefaultQueueSize, // some default
	}
	subs := make(map[string]MqttMsgHandler)
	subs[fmt.Sprintf("/devices/%d/command", dc.DeviceID)] = del.handleCommandMsg
	subs[fmt.Sprintf("/devices/%d/kv", dc.DeviceID)] = del.handleKeyValueMsg

	del.subscriptions = subs

	for _, opt := range opts {
		opt(del)
	}
	// Set the channels after all the options have been set.
	del.command = make(chan *DeviceCommand, del.sendQueueSize)
	del.kvSync = make(chan *KeyValueSync, del.sendQueueSize)
	return del
}

func (del *ModeMqttDelegate) GetCommandChannel() chan *DeviceCommand {
	return del.command
}

func (del *ModeMqttDelegate) GetKVSyncChannel() chan *KeyValueSync {
	return del.kvSync
}

func (del *ModeMqttDelegate) GetDeviceContext() *DeviceContext {
	return del.dc
}

func (del *ModeMqttDelegate) StartSubscriptionListener() {
	del.stopSubCh = make(chan bool)
	go del.runSubscriptionListener()
}

// Non-MqttDelegate method to listen on subscriptions and pass the interpreted
// data to the appropriate channel (kvSync or command)
func (del *ModeMqttDelegate) runSubscriptionListener() {

	for {
		select {
		case subData := <-del.SubRecvCh:
			subBytes := subData.Data
			// Determine which callback to call based on the topic
			if handler, exists := del.subscriptions[subData.Topic]; exists {
				if err := handler(subBytes); err != nil {
					logError("Error in subscription handler: %s", err)
				}
			} else {
				logError("No subscription handler for %s", subData.Topic)
			}
		case <-del.stopSubCh:
			return
		}
	}
}

func (del *ModeMqttDelegate) TLSUsageAndConfiguration() (useTLS bool,
	tlsConfig *tls.Config) {
	return del.UseTLS, del.dc.TLSConfig
}

func (del *ModeMqttDelegate) AuthInfo() (username string, password string) {
	if del.dc.TLSClientAuth {
		return strconv.FormatUint(del.dc.DeviceID, 10), ""
	}
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

func (del *ModeMqttDelegate) Subscriptions() []string {
	keys := make([]string, len(del.subscriptions))

	i := 0
	for k := range del.subscriptions {
		keys[i] = k
		i++
	}
	return keys
}

func (del *ModeMqttDelegate) OnClose() {
	if del.stopSubCh != nil {
		del.stopSubCh <- true
	}
	close(del.command)
	close(del.kvSync)
	// By default, there are no listeners for the PingAckCh and QueueAckCh. If
	// there are goroutines listening to those channels, this is where they can
	// be signaled to stop
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

	del.kvSync <- &kvSync
	return nil
}

// Mode extensions to the MqttClient
// cast to the concrete delegate
func (client *MqttClient) GetModeAuthDelegate() (*ModeMqttDelegate, error) {
	// Since we are implementing all the delegates in one, we could cast from
	// any of them.
	implDelegate, ok := client.authDelegate.(*ModeMqttDelegate)
	if !ok {
		return implDelegate, fmt.Errorf("MqttClient was not created with Mode Delegate")
	}
	return implDelegate, nil
}

// Helper function to send DeviceEvent instances
func (client *MqttClient) PublishEvent(ctx context.Context,
	event DeviceEvent) (uint16, error) {
	modeDel, err := client.GetModeAuthDelegate()
	if err != nil {
		return 0, err
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	topic := fmt.Sprintf("/devices/%d/event", modeDel.dc.DeviceID)
	return client.Publish(ctx, event.Qos, topic, payload)
}

// Helper function to send DeviceEvent instances. This replaces both the
// sendBulkData and writeBulkData methods in the old API (since it does less
// than both, covering just the intersection)
func (client *MqttClient) PublishBulkData(ctx context.Context,
	bulkData DeviceBulkData) (uint16,
	error) {
	modeDel, err := client.GetModeAuthDelegate()
	if err != nil {
		return 0, err
	}

	topic := fmt.Sprintf("/devices/%d/bulkData/%s", modeDel.dc.DeviceID,
		bulkData.StreamID)

	return client.Publish(ctx, bulkData.Qos, topic, bulkData.Blob)
}

// The key value store should typically be cached. Key Values are all sent on
// subscription, so should be handled in the client by receiving on the kvSync
// channel. There is no method of fetching single key value pairs. We only
// update the key values.
// XXX: Looks like there's a reload, so I must be missing something.
func (client *MqttClient) PublishKeyValueUpdate(ctx context.Context,
	kvData KeyValueSync) (uint16,
	error) {
	modeDel, err := client.GetModeAuthDelegate()
	if err != nil {
		return 0, err
	}

	payload, err := json.Marshal(kvData)
	if err != nil {
		return 0, err
	}
	topic := fmt.Sprintf("/devices/%d/kv", modeDel.dc.DeviceID)

	// Hardcode QOS1
	return client.Publish(ctx, QOSAtLeastOnce, topic, payload)
}

// A special JSON decoder that makes sure numbers in command parameters
// are preserved (avoid turning integers into floats).
func decodeOpaqueJSON(b []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	return decoder.Decode(v)
}
