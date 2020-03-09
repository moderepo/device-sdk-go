/*
This is Version 3 of the package of the go API for devices to interact with the MODE cloud.

You can connect to the cloud in three steps:

1. Construct a DeviceContext
  deviceContext := &mode.DeviceContext{...}
2. Use the DeviceContext to construct Mode's implementation of the MqttDelegate.
  delegate := mode.NewModeMqttDelegate(...)
3. Create a client using the delegate
  client := mode.NewMqttClient(mqttHost, mqttPort, WithMqttDelegate(delegate))

Customization and configuration are done through various delegate interfaces. For
 convenience, there is a general delegate (MqttDelegate), as used in the example, which
 contains all the delegate interfaces except the error delegate. The ModeMqttDelegate is
 implemented to subscribe and receive the MQTT topics used by MODE, but the MqttClient can
 be constructed with other implementations to communicate with other MQTT servers.

Reqeusts to the cloud through the MqttClient fall in two categories: non-blocking, or
 blocking until a response is received. Refer to the documentation for details. For
non-blocking calls, the MqttReceiverDelegate will receive the response channels from the
 client via the SetReceiveChannels().

*/
package mode

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"time"

	"golang.org/x/crypto/pkcs12"
)

const (
	sdkVersion      = "3.0"
	defaultRESTHost = "api.tinkermode.com"
)

var (
	restHost = defaultRESTHost
	restPort = 443
	restTLS  = true
)

// SetRESTHostPort overrides the default REST API server host and port, and specifies
// whether TLS connection should be used. (TLS is used by default.)
func SetRESTHostPort(host string, port int, useTLS bool) {
	restHost = host
	restPort = port
	restTLS = useTLS
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

// BindParameters maps the command parameters from JSON to the provided struct.
func (cmd *DeviceCommand) BindParameters(v interface{}) error {
	if cmd.payload == nil {
		// nothing to do.
		return nil
	}

	return json.Unmarshal(cmd.payload, v)
}
