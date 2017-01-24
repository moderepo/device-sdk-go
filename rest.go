package mode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	defaultHTTPTimeout          = time.Second * 10
	defaultHTTPKeepAliveTimeout = time.Minute * 5
	defaultHTTPKeepAliveMaxConn = 5
	defaultUserAgent            = "GoDeviceSDK/" + sdkVersion
)

var (
	httpTransport = &http.Transport{
		MaxIdleConnsPerHost: defaultHTTPKeepAliveMaxConn,
		IdleConnTimeout:     defaultHTTPKeepAliveTimeout,
	}

	httpClient = &http.Client{
		Timeout: defaultHTTPTimeout,
	}
)

// RESTError represents an error returned by the MODE REST API.
type RESTError struct {
	statusCode int
	reason     string
	data       map[string]interface{}
}

func newRESTError(statusCode int, jsonBytes []byte) *RESTError {
	restErr := &RESTError{statusCode: statusCode}

	var fields struct {
		Reason string                 `json:"reason"`
		Data   map[string]interface{} `json:"data"`
	}

	if err := json.Unmarshal(jsonBytes, &fields); err == nil {
		restErr.reason = fields.Reason
		restErr.data = fields.Data
	} else {
		restErr.reason = "UNKNOWN"
	}

	return restErr
}

// Error returns a summary of the error.
func (e *RESTError) Error() string {
	return fmt.Sprintf("server returned status %d (reason: %s)", e.statusCode, e.reason)
}

// Reason returns the specific reason for the error.
func (e *RESTError) Reason() string {
	return e.reason
}

// StatusCode returns the HTTP status code provided by the API server.
func (e *RESTError) StatusCode() int {
	return e.statusCode
}

// Data returns any additional data associated with this error, or nil.
func (e *RESTError) Data() map[string]interface{} {
	return e.data
}

func makeRESTCallURL(path string) string {
	var proto string
	if restTLS {
		proto = "https"
	} else {
		proto = "http"
	}

	return fmt.Sprintf("%s://%s:%d%s", proto, restHost, restPort, path)
}

func makeRESTAuth(authToken string) string {
	return fmt.Sprintf("ModeCloud %s", authToken)
}

func makeRESTCall(method string, path string, authToken string, data interface{}, resData interface{}) error {
	url := makeRESTCallURL(path)

	var body io.Reader
	if data != nil {
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			logError("Failed to encode data to JSON: %s", err.Error())
			return err
		}

		body = bytes.NewReader(jsonBytes)
	}

	request, err := http.NewRequest(method, url, body)
	if err != nil {
		logError("Failed to set up HTTP request: %s", err.Error())
		return err
	}

	request.Header.Add("User-Agent", defaultUserAgent)

	if authToken != "" {
		request.Header.Add("Authorization", makeRESTAuth(authToken))
	}

	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	res, err := httpClient.Do(request)
	if err != nil {
		logError("Failed to make HTTP request: %s", err.Error())
		return err
	}
	defer res.Body.Close()

	jsonBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		logError("Failed to read HTTP response body: %s", err.Error())
		return err
	}

	logInfo("REST API response code %d", res.StatusCode)

	if res.StatusCode == http.StatusNoContent {
		return nil
	}

	if res.StatusCode == http.StatusOK || res.StatusCode == http.StatusCreated {
		if resData == nil {
			return nil
		} else {
			if err := json.Unmarshal(jsonBytes, resData); err == nil {
				return nil
			} else {
				logError("Failed to parse JSON response: %s", err.Error())
				return err
			}
		}
	}

	return newRESTError(res.StatusCode, jsonBytes)
}
