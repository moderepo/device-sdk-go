package mode

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsHandshakeTimeout = time.Second * 10
	wsCtrlMsgTimeout   = time.Second * 3
)

var wsDialer = websocket.Dialer{
	HandshakeTimeout: wsHandshakeTimeout,
}

type (
	wsConn struct {
		conn          *websocket.Conn
		dc            *DeviceContext
		command       chan<- *DeviceCommand
		event         <-chan *DeviceEvent
		err           chan error
		doPing        chan time.Duration
		pong          chan string
		stopEventProc chan bool
		wgWrite       sync.WaitGroup
		wgRead        sync.WaitGroup
	}
)

func wsParseMessage(msgType int, data []byte) (*DeviceCommand, error) {
	if msgType != websocket.TextMessage {
		return nil, fmt.Errorf("unsupported message type: %d", msgType)
	}

	var cmd struct {
		Action     string                 `json:"action"`
		Parameters map[string]interface{} `json:"parameters"`
	}

	if err := decodeOpaqueJSON(data, &cmd); err != nil {
		return nil, fmt.Errorf("message data is not valid command JSON: %s", err.Error())
	}

	if cmd.Action == "" {
		return nil, errors.New("message data is not valid command JSON: no action field")
	}

	// Re-encode parameters into JSON payload for later use.
	var payload []byte
	if cmd.Parameters != nil {
		payload, _ = json.Marshal(cmd.Parameters)
	}

	return &DeviceCommand{Action: cmd.Action, payload: payload}, nil
}

func (dc *DeviceContext) makeWebsocketURL() string {
	var proto string
	if restTLS {
		proto = "wss"
	} else {
		proto = "ws"
	}

	return fmt.Sprintf("%s://%s:%d/devices/%d/command", proto, restHost, restPort, dc.DeviceID)
}

func (ws *wsConn) runReader() {
	logInfo("[WS] reader is running")
	ws.wgRead.Add(1)

	defer func() {
		logInfo("[WS] reader is exiting")
		ws.wgRead.Done()
	}()

	for {
		msgType, data, err := ws.conn.ReadMessage()
		if err != nil {
			logError("[WS] failed to read message: %s", err.Error())
			ws.err <- err
			break
		}

		if cmd, err := wsParseMessage(msgType, data); err == nil {
			ws.command <- cmd
		} else {
			logError("[WS] received invalid message: %s", err.Error())
		}
	}

}

func (ws *wsConn) runEventProcessor() {
	logInfo("[WS] event processor is running")
	ws.wgWrite.Add(1)

	defer func() {
		logInfo("[WS] event processor is exiting")
		ws.wgWrite.Done()
	}()

	for {
		select {
		case <-ws.stopEventProc:
			return

		case e := <-ws.event:
			if err := ws.sendEvent(e); err != nil {
				logError("[WS] failed to send event: %s", err.Error())
			}
		}
	}
}

func (ws *wsConn) sendEvent(e *DeviceEvent) error {
	var maxAttempts uint

	switch e.qos {
	case QOSAtMostOnce:
		maxAttempts = 1
	case QOSAtLeastOnce:
		maxAttempts = maxDeviceEventAttempts
	default:
		return errors.New("unsupported qos level")
	}

	for count := uint(1); count <= maxAttempts; count++ {
		logInfo("[WS] event delivery attempt #%d", count)

		// We actually don't use websocket to send outbound events. Instead we just use the REST API.
		err := makeRESTCall("PUT", fmt.Sprintf("/devices/%d/event", ws.dc.DeviceID), ws.dc.AuthToken, e, nil)
		if err == nil {
			logInfo("[WS] successfully sent event %s", e.EventType)
			return nil
		}

		logError("[WS] failed to send event %s: %s", e.EventType, err.Error())

		if count < maxAttempts {
			logInfo("[WS] will retry sending event in %v", deviceEventRetryInterval)
			time.Sleep(deviceEventRetryInterval)
		}
	}

	return errors.New("event dropped")
}

func (ws *wsConn) runPinger() {
	logInfo("[WS] pinger is running")
	ws.wgWrite.Add(1)

	defer func() {
		logInfo("[WS] pinger is exiting")
		ws.wgWrite.Done()
	}()

	var data struct {
		ID        uint      `json:"id"`
		Timestamp time.Time `json:"ts"`
	}

	for pingTimeout := range ws.doPing {
		data.ID += 1
		data.Timestamp = time.Now()
		dataBuf, _ := json.Marshal(data)

		deadline := time.Now().Add(wsCtrlMsgTimeout)

		if err := ws.conn.WriteControl(websocket.PingMessage, dataBuf, deadline); err != nil {
			logError("[WS] failed to send PING control msg: %s", err.Error())
			ws.err <- errors.New("failed to send ping")
			continue
		}

		logInfo("[WS] sent PING control msg with data %s", string(dataBuf))
		logInfo("[WS] waiting for PONG control msg")

		select {
		case <-time.After(pingTimeout):
			logError("[WS] did not receive PONG within %v", pingTimeout)
			ws.err <- errors.New("ping timeout")

		case data := <-ws.pong:
			logInfo("[WS] received PONG control msg with data %s", data)
		}
	}
}

func (ws *wsConn) close() {
	close(ws.stopEventProc) // tell event processor to quit
	close(ws.doPing)        // tell pinger to quit

	ws.wgWrite.Wait() // wait for event processor and pinger to finish

	data := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "device initiated closure")
	deadline := time.Now().Add(wsCtrlMsgTimeout)

	if err := ws.conn.WriteControl(websocket.CloseMessage, data, deadline); err != nil {
		logError("[WS] failed to send CLOSE control msg: %s", err.Error())
	}

	/*
		Closing the websocket connection here should cause any blocking read()
		call to fail, thus forcing the reader to exit. But sometimes the read
		operation may be stuck if there is some network issue. Setting a read
		deadline here may force the read() call to fail.
	*/
	ws.conn.SetReadDeadline(time.Now().Add(time.Millisecond * 500))
	ws.conn.Close()
	ws.wgRead.Wait() // wait for reader to finish
}

func (ws *wsConn) getErrorChan() chan error {
	return ws.err
}

func (ws *wsConn) ping(timeout time.Duration) {
	ws.doPing <- timeout
}

func (dc *DeviceContext) openWebsocket(cmdQueue chan<- *DeviceCommand, evtQueue <-chan *DeviceEvent) (*wsConn, error) {
	url := dc.makeWebsocketURL()
	header := http.Header{}
	header.Add("Authorization", makeRESTAuth(dc.AuthToken))

	conn, res, err := wsDialer.Dial(url, header)
	if err != nil {
		logError("Websocket dialer failed: %s", err.Error())
		return nil, err
	}

	logInfo("Websocket response status code: %d", res.StatusCode)

	ws := &wsConn{
		conn:          conn,
		dc:            dc,
		command:       cmdQueue,
		event:         evtQueue,
		err:           make(chan error, 10), // make sure this won't block
		doPing:        make(chan time.Duration, 1),
		pong:          make(chan string, 1),
		stopEventProc: make(chan bool),
	}

	conn.SetPongHandler(func(appData string) error {
		ws.pong <- appData
		return nil
	})

	go ws.runReader()
	go ws.runEventProcessor()
	go ws.runPinger()

	return ws, nil
}
