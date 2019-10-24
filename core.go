/*
This package provides a Go API for devices to interact with the MODE cloud.

If a device wants to receive commands from and send events to the MODE cloud,
it must start a connection session. Connection session is also required for the
device to use the Device Data Proxy (device key-value store) feature.

Both incoming commands and outgoing events are queued. If the connection is
disrupted, commands already in the queue will be processed. Likewise,
events already in the queue will be delivered when the connection resumes.
*/
package mode

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"golang.org/x/crypto/pkcs12"
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

	// For publishing device events to cloud.
	deviceEventRetryInterval    = time.Second * 15
	syncedBulkDataRetryInterval = time.Second * 15

	// For publishing key-value events to cloud.
	maxKeyValueUpdateAttempts   uint = 3
	keyValueUpdateRetryInterval      = time.Second * 5

	// TBD: how much buffering should we allow?
	eventQueueLength            = 128
	bulkDataQueueLength         = 128
	commandQueueLength          = 128
	keyValueSyncQueueLength     = 128
	keyValuePushQueueLength     = 128
	keyValueCallbackQueueLength = 128
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

// Deprecated: Use ConfigureDeviceEventTimeout
// ConfigureDeviceEventSender overrides the default parameters used by the
// device event sender. These config parameters are used when sending device
// events with QoS1 (at least once).
func ConfigureDeviceEventSender(_ uint, retryInterval time.Duration) {
	deviceEventRetryInterval = retryInterval
}

// ConfigureDeviceEventTimeout overrides the default parameters used by the
// timeout of device event sender. These config parameters are used when sending device
// events with QoS1 (at least once).
func ConfigureDeviceEventTimeout(retryInterval time.Duration) {
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
	// If you want to use client certificate instead of AuthToken,
	// set TLSClientAuth to true and call SetPKCS12ClientCertificate function.
	DeviceContext struct {
		DeviceID      uint64
		AuthToken     string
		TLSClientAuth bool
		TLSConfig     *tls.Config
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
		payload json.RawMessage
	}

	// DeviceEvent represents an event to be sent to the MODE cloud.
	DeviceEvent struct {
		EventType string                 `json:"eventType"`
		EventData map[string]interface{} `json:"eventData,omitempty"`
		qos       QOSLevel               // not exported to JSON
	}

	// BulkData represents a batch of opaque data to be sent to the MODE cloud.
	DeviceBulkData struct {
		StreamID string
		Blob     []byte
		qos      QOSLevel // not exported to serializer
	}

	// BulkData represents a batch of opaque data to be sent to the MODE cloud.
	DeviceSyncedBulkData struct {
		StreamID string
		Blob     []byte
		response chan error // not exported to serializer
	}

	// A callback function that handles a device command.
	CommandHandler func(*DeviceContext, *DeviceCommand)

	// KeyValue represents a key-value pair stored in the Device Data Proxy.
	KeyValue struct {
		Key              string      `json:"key"`
		Value            interface{} `json:"value"`
		ModificationTime time.Time   `json:"modificationTime"`
	}

	// A callback function that is invoked when the Device Data Proxy is ready
	// to be accessed.
	KeyValuesReadyCallback func(*DeviceContext)

	// A callback function that is invoked when a key-value pair has been added
	// or updated.
	KeyValueStoredCallback func(*DeviceContext, *KeyValue)

	// A callback function that is invoked when a key-value pair has been deleted.
	// The key of the deleted key-value is passed in the argument.
	KeyValueDeletedCallback func(*DeviceContext, string)
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

	if err := makeRESTCall("POST", "/devices", nil, &params, &resData); err != nil {
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

	return makeRESTCall("POST", "/deviceRegistration", dc, &params, nil)
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

	return makeRESTCall("POST", "/deviceRegistration", dc, &params, nil)
}

// GetInfo fetches the device's information from MODE.
func (dc *DeviceContext) GetInfo() (*DeviceInfo, error) {
	d := &DeviceInfo{}
	if err := makeRESTCall("GET", fmt.Sprintf("/devices/%d", dc.DeviceID), dc, nil, d); err != nil {
		return nil, err
	}

	return d, nil
}

// SetPKCS12ClientCertificate set PKCS#12 client certificate to device context.
// Set fileName and password of the certificate. If insecureSkipVerify is true,
// TLS accepts any certificate presented by the server and any host name in
// that certificate. This should be used only for testing.
func (dc *DeviceContext) SetPKCS12ClientCertificate(fileName string, password string, insecureSkipVerify bool) error {
	p12Content, err := ioutil.ReadFile(fileName)
	if err != nil {
		logError("Read PKCS#12 file failed: %v", err)
		return err
	}

	p12, err := base64.StdEncoding.DecodeString(string(p12Content))
	if err != nil {
		logError("PKCS#12 file should be base64 encoded: %v", err)
		return err
	}

	blocks, err := pkcs12.ToPEM(p12, password)
	if err != nil {
		logError("Read PKCS#12 file failed: %v", err)
		return err
	}

	var pemData []byte
	for _, b := range blocks {
		pemData = append(pemData, pem.EncodeToMemory(b)...)
	}

	// then use PEM data for tls to construct tls certificate:
	cert, err := tls.X509KeyPair(pemData, pemData)
	if err != nil {
		logError("Parse X509KeyPair failed: %v", err)
		return err
	}

	dc.TLSConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: insecureSkipVerify,
	}

	return nil
}

func (cmd *DeviceCommand) String() string {
	if cmd.payload == nil {
		return fmt.Sprintf("{Action:\"%s\"}", cmd.Action)
	} else {
		return fmt.Sprintf("{Action:\"%s\", Parameters:%s}", cmd.Action, string(cmd.payload))
	}
}

func (kv *KeyValue) String() string {
	return fmt.Sprintf("{Key:\"%s\", Value:%v, LastModified:%s}", kv.Key, kv.Value, kv.ModificationTime.Format(time.RFC3339))
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
