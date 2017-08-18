/*
This package provides a Go API for devices to interact with the MODE cloud.

If a device wants to receive commands from and send events to the MODE cloud,
it must start a connection session. You can choose to connect via HTTP/websocket
or MQTT.

Both incoming commands and outgoing events are queued. If the websocket or MQTT
connection is disrupted, commands already in the queue will be processed. Likewise,
events already in the queue will be delivered when the connection resumes.
*/
package mode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	sdkVersion      = "1.0"
	defaultRESTHost = "api.tinkermode.com"
	defaultMQTTHost = "mqtt.tinkermode.com"
)

// QoS level of message delivery. This is used in sending events to MODE.
type QOSLevel int

const (
	// QoS 0 - message delivery is not guaranteed.
	QOSAtMostOnce QOSLevel = iota

	// QoS 1 - message is delivered at least once, but duplicates may happen.
	QOSAtLeastOnce

	// QoS 2 - message is always delivered exactly once. This is currently not supported.
	QOSExactlyOnce
)

var (
	restHost = defaultRESTHost
	restPort = 443
	restTLS  = true

	mqttHost = defaultMQTTHost
	mqttPort = 8883
	mqttTLS  = true

	infoLogger  = log.New(os.Stdout, "[MODE - INFO] ", log.LstdFlags)
	errorLogger = log.New(os.Stderr, "[MODE - ERROR] ", log.LstdFlags)

	maxDeviceEventAttempts      uint = 3
	maxDeviceKeyValueAttempts   uint = 3
	deviceEventRetryInterval         = time.Second * 5
	deviceKeyValueRetryInterval      = time.Second * 5

	// TBD: how much buffering should we allow?
	eventQueueLength    = 128
	keyValueQueueLength = 128
	commandQueueLength  = 128
)

// SetRESTHostPort overrides the default REST API server host and port, and specifies
// whether TLS connection should be used. (TLS is used by default.)
func SetRESTHostPort(host string, port int, useTLS bool) {
	restHost = host
	restPort = port
	restTLS = useTLS
}

// SetMQTTHostPort overrides the default MQTT server host and port, and specifies
// whether TLS connection should be used. (TLS is used by default.)
func SetMQTTHostPort(host string, port int, useTLS bool) {
	mqttHost = host
	mqttPort = port
	mqttTLS = useTLS
}

// SetInfoLogger overrides the default debug logger, which writes to STDOUT.
func SetInfoLogger(l *log.Logger) {
	infoLogger = l
}

// SetErrorLogger overrides the default error logger, which writes to STDERR.
func SetErrorLogger(l *log.Logger) {
	errorLogger = l
}

// ConfigureDeviceEventSender overrides the default parameters used by the
// device event sender. These config parameters are used when sending device
// events with QoS1 (at least once).
func ConfigureDeviceEventSender(maxAttempts uint, retryInterval time.Duration) {
	maxDeviceEventAttempts = maxAttempts
	deviceEventRetryInterval = retryInterval
}

func logInfo(format string, values ...interface{}) {
	infoLogger.Printf(format+"\n", values...)
}

func logError(format string, values ...interface{}) {
	errorLogger.Printf(format+"\n", values...)
}

type (
	// An initialized DeviceContext is needed for most API calls. Normally,
	// DeviceID and AuthToken are provisioned using the MODE Developer Console.
	// If on-demand device provisioning is enabled for your MODE project, you
	// can call ProvisionDevice to create a new DeviceContext.
	DeviceContext struct {
		DeviceID  uint64
		AuthToken string
	}

	// DeviceInfo contains the key information fetched from the MODE API.
	DeviceInfo struct {
		ID          uint64 `json:"id"`
		ProjectID   uint64 `json:"projectId"`
		Name        string `json:"name"`
		Tag         string `json:"tag"`
		DeviceClass string `json:"deviceClass"`
	}

	// DeviceCommand represents a command received from the MODE cloud.
	DeviceCommand struct {
		Action  string
		payload []byte
	}

	// DeviceEvent represents an event to be sent to the MODE cloud.
	DeviceEvent struct {
		EventType string                 `json:"eventType"`
		EventData map[string]interface{} `json:"eventData,omitempty"`
		qos       QOSLevel               // not exported to JSON
	}

	ActionKeyValue struct {
		Action  string                 `json:"action"`
		Key     string                 `json:"key"`
		Value   map[string]interface{} `json:"value"`
		Rev     int                    `json:"rev"`
		Items   []interface{}          `json:"items"`
		MTime   time.Time              `json:"mtime"`
		Payload []byte
	}

	KeyValue struct {
		Key   string                 `json:"key"`
		Value map[string]interface{} `json:"value"`
	}

	// A callback function that handles a device command.
	CommandHandler func(*DeviceContext, *DeviceCommand)

	KvReloadHandler func(*DeviceContext, []*KeyValue) bool
	KvSetHandler    func(*DeviceContext, *KeyValue) bool
	KvDeleteHandler func(*DeviceContext, string) bool
)

// ProvisionDevice is used for on-demand device provisioning. It takes a
// provisioning token which is obtained by the user who initiated the process.
// If successful, the device should store the returned DeviceContext for all
// future API calls.
func ProvisionDevice(token string) (*DeviceContext, error) {
	var params struct {
		Token string `json:"token"`
	}

	var resData struct {
		ID     uint64 `json:"id"`
		APIKey string `json:"apiKey"`
	}

	params.Token = token

	if err := makeRESTCall("POST", "/devices", "", &params, &resData); err != nil {
		return nil, err
	}

	return &DeviceContext{DeviceID: resData.ID, AuthToken: resData.APIKey}, nil
}

func (d *DeviceInfo) String() string {
	return fmt.Sprintf("%s{ID:%d, Name:\"%s\", Tag:\"%s\"}", d.DeviceClass, d.ID, d.Name, d.Tag)
}

// EnableClaimMode activates the device's "claim mode", i.e. allows the device to
// be added to a different home. The claim mode will be active for the time period
// specified by "duration".
func (dc *DeviceContext) EnableClaimMode(duration time.Duration) error {
	var params struct {
		DeviceID  uint64 `json:"deviceId"`
		Claimable bool   `json:"claimable"`
		Duration  uint64 `json:"duration"` // in seconds
	}

	params.DeviceID = dc.DeviceID
	params.Claimable = true
	params.Duration = uint64(duration / time.Second)

	return makeRESTCall("POST", "/deviceRegistration", dc.AuthToken, &params, nil)
}

// DisableClaimMode turns off the device's "claim mode", disallowing it to be added
// to a different home.
func (dc *DeviceContext) DisableClaimMode() error {
	var params struct {
		DeviceID  uint64 `json:"deviceId"`
		Claimable bool   `json:"claimable"`
	}

	params.DeviceID = dc.DeviceID
	params.Claimable = false

	return makeRESTCall("POST", "/deviceRegistration", dc.AuthToken, &params, nil)
}

// GetInfo fetches the device's information from MODE.
func (dc *DeviceContext) GetInfo() (*DeviceInfo, error) {
	d := &DeviceInfo{}
	if err := makeRESTCall("GET", fmt.Sprintf("/devices/%d", dc.DeviceID), dc.AuthToken, nil, d); err != nil {
		return nil, err
	}

	return d, nil
}

func (cmd *DeviceCommand) String() string {
	if cmd.payload == nil {
		return fmt.Sprintf("{Action:\"%s\"}", cmd.Action)
	} else {
		return fmt.Sprintf("{Action:\"%s\", Parameters:%s}", cmd.Action, string(cmd.payload))
	}
}

// A special JSON decoder that makes sure numbers in command parameters
// are preserved (avoid turning integers into floats).
func decodeOpaqueJSON(b []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	return decoder.Decode(v)
}

// BindParameters maps the command parameters from JSON to the provided struct.
func (cmd *DeviceCommand) BindParameters(v interface{}) error {
	if cmd.payload == nil {
		// nothing to do.
		return nil
	}

	return json.Unmarshal(cmd.payload, v)
}
