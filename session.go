package mode

import (
	"errors"
	"strings"
	"time"
)

type (
	connection interface {
		close()
		getErrorChan() chan error
		ping(timeout time.Duration)
	}

	session struct {
		dc                             *DeviceContext
		conn                           connection
		cmdQueue                       chan *DeviceCommand
		evtQueue                       chan *DeviceEvent
		bulkDataQueue                  chan *DeviceBulkData
		syncedBulkDataCh               chan *DeviceSyncedBulkData
		bulkDataRequestQueue           chan *DeviceBulkDataRequest
		bulkDataResponseQueue          chan *DeviceBulkDataResponse
		subscribeBulkDataResponseQueue chan *DeviceSubscribeBulkDataResponse
		kvCache                        *keyValueCache
	}

	sessCtrlStart struct {
		dc       *DeviceContext
		response chan error
	}

	sessCtrlStop struct {
		response chan error
	}

	sessCtrlGetState struct {
		response chan SessionState
	}

	sessCtrlSendEvent struct {
		event    *DeviceEvent
		response chan error
	}

	sessCtrlSendBulkData struct {
		event    *DeviceBulkData
		response chan error
	}

	sessCtrlWriteBulkData struct {
		event    *DeviceSyncedBulkData
		response chan error
	}

	sessCtrlSendBulkDataRequest struct {
		request  *DeviceBulkDataRequest
		response chan error
	}

	sessCtrlSubscribeBulkDataResponse struct {
		subscribe *DeviceSubscribeBulkDataResponse
		response  chan error
	}

	sessCtrlSendBulkDataResponse struct {
		request  *DeviceBulkDataResponse
		response chan error
	}

	kvAction int

	sessCtrlAccessKV struct {
		action      kvAction
		key         string
		value       interface{}
		responseKV  chan *KeyValue
		responseErr chan error
	}

	// SessionState represents a state of the device's connection session.
	SessionState int

	// A callback function that is invoked when the device's connection session
	// changes state.
	SessionStateCallback func(SessionState)
)

const (
	kvActionGet kvAction = iota
	kvActionGetAll
	kvActionSet
	kvActionDelete
)

const (
	sessMaxRetryInterval    = time.Second * 32
	sessDefaultPingInterval = time.Minute
	sessDefaultPingTimeout  = time.Second * 10
)

const (
	// Device is currently not connected to the MODE cloud.
	SessionIdle SessionState = iota

	// Device is currently connected to the MODE cloud.
	SessionActive

	// Connection to the MODE cloud has been disrupted and is waiting to
	// be re-established.
	SessionRecovering
)

var (
	sessState = SessionIdle

	sessCtrl struct {
		start                     chan *sessCtrlStart
		stop                      chan *sessCtrlStop
		getState                  chan *sessCtrlGetState
		sendEvent                 chan *sessCtrlSendEvent
		sendBulkData              chan *sessCtrlSendBulkData
		writeBulkData             chan *sessCtrlWriteBulkData
		accessKV                  chan *sessCtrlAccessKV
		sendBulkDataRequest       chan *sessCtrlSendBulkDataRequest
		subscribeBulkDataResponse chan *sessCtrlSubscribeBulkDataResponse
		ping                      <-chan time.Time
	}

	sess *session

	commandHandlers       = map[string]CommandHandler{}
	defaultCommandHandler CommandHandler

	sessStateCallback SessionStateCallback

	keyValuesReadyCallback  KeyValuesReadyCallback
	keyValueStoredCallback  KeyValueStoredCallback
	keyValueDeletedCallback KeyValueDeletedCallback

	bulkDataResponseReceivers = map[string]BulkDataResponseReceiver{}

	sessPingInterval = sessDefaultPingInterval
	sessPingTimeout  = sessDefaultPingTimeout
)

var (
	ErrorSessionAlreadyStarted = errors.New("session already started")
	ErrorSessionNotStarted     = errors.New("not in session")
	ErrorSessionRecovering     = errors.New("session is recovering")
)

func init() {
	sessCtrl.start = make(chan *sessCtrlStart, 1)
	sessCtrl.stop = make(chan *sessCtrlStop, 1)
	sessCtrl.getState = make(chan *sessCtrlGetState, 1)
	sessCtrl.sendEvent = make(chan *sessCtrlSendEvent, 1)
	sessCtrl.sendBulkData = make(chan *sessCtrlSendBulkData, 1)
	sessCtrl.writeBulkData = make(chan *sessCtrlWriteBulkData, 1)
	sessCtrl.sendBulkDataRequest = make(chan *sessCtrlSendBulkDataRequest, 1)
	sessCtrl.subscribeBulkDataResponse = make(chan *sessCtrlSubscribeBulkDataResponse, 1)
	sessCtrl.accessKV = make(chan *sessCtrlAccessKV, 1)
	sessCtrl.ping = time.Tick(sessPingInterval)

	go runSessionManager()
}

// SetCommandHandler assigns a function to handle device commands
// coming from the MODE cloud with the specified action.
//
// IMPORTANT: Incoming device commands are queued and handled serially by
// a goroutine. In your handler function, you should decide whether to spawn
// goroutines to do certain work.
func SetCommandHandler(action string, h CommandHandler) {
	commandHandlers[strings.ToLower(action)] = h
}

// SetDefaultCommandHandler assigns a function to handle device
// commands that don't have dedicated handlers. If default command
// handler is not set, these unhandled commands will be dropped.
//
// IMPORTANT: Incoming device commands are queued and handled serially by
// a goroutine. In your handler function, you should decide whether to spawn
// goroutines to do certain work.
func SetDefaultCommandHandler(h CommandHandler) {
	defaultCommandHandler = h
}

// SetSessionStateCallback designates a function to be called when
// the device's connection session changes state.
func SetSessionStateCallback(f SessionStateCallback) {
	sessStateCallback = f
}

// SetKeyValuesReadyCallback designates a function to be called when the Device
// Data Proxy is ready to be accessed.
//
// IMPORTANT: key-value callbacks are queued and executed serially by a goroutine.
// In your callback function, you should decide whether to spawn goroutines to
// do certain work.
func SetKeyValuesReadyCallback(f KeyValuesReadyCallback) {
	keyValuesReadyCallback = f
}

// SetKeyValueStoredCallback designates a function to be called whenever a
// key-value pair has been added or updated by someone else.
//
// IMPORTANT: key-value callbacks are queued and executed serially by a goroutine.
// In your callback function, you should decide whether to spawn goroutines to
// do certain work.
func SetKeyValueStoredCallback(f KeyValueStoredCallback) {
	keyValueStoredCallback = f
}

// SetKeyValueDeletedCallback designates a function to be called whenever a
// key-value pair has been deleted by someone else.
//
// IMPORTANT: key-value callbacks are queued and executed serially by a goroutine.
// In your callback function, you should decide whether to spawn goroutines to
// do certain work.
func SetKeyValueDeletedCallback(f KeyValueDeletedCallback) {
	keyValueDeletedCallback = f
}

// SetBulkDataResponseReceiver register a callback function when it received request from a DataStream.
func SetBulkDataResponseReceiver(streamID string, receiver BulkDataResponseReceiver) error {
	bulkDataResponseReceivers[strings.ToLower(streamID)] = receiver

	ctrl := &sessCtrlSubscribeBulkDataResponse{
		subscribe: &DeviceSubscribeBulkDataResponse{StreamID: streamID},
		response:  make(chan error, 1),
	}

	sessCtrl.subscribeBulkDataResponse <- ctrl
	return <-ctrl.response
}

func resubscribeBulkDataResponse() error {
	logInfo("[Session] Start re-setting SubscribeBulkDataResponse")
	defer logInfo("[Session] Finished re-setting SubscribeBulkDataResponse")

	oldReceivers := bulkDataResponseReceivers
	// Re-create map object
	bulkDataResponseReceivers = map[string]BulkDataResponseReceiver{}
	// Copy
	for streamID, receiver := range oldReceivers {
		bulkDataResponseReceivers[streamID] = receiver
	}
	// SetBulkDataResponseReceiver has side effect for bulkDataResponseReceivers map.
	// So loop uses coped map object.
	// And if MQTT happened error then it losts topic and receiver from map.
	// So bulkDataResponseReceivers is created the new map object and it is copied map
	// from old map at previous lines.
	for streamID, receiver := range oldReceivers {
		if err := SetBulkDataResponseReceiver(streamID, receiver); err != nil {
			logError("[Session] Re subscriber failed to subsrive %s: %v", streamID, err)
			return err
		}
	}
	return nil
}

// When the device's connection session is in the "active" state, "pings"
// are periodically sent to the server. A ping fails if the server doesn't
// respond. ConfigurePings overrides the default time interval between pings and
// the timeout for the server's responses to pings.
func ConfigurePings(interval time.Duration, timeout time.Duration) {
	sessPingInterval = interval
	sessPingTimeout = timeout
}

func initSession(dc *DeviceContext) error {
	sess = &session{
		dc:                             dc,
		cmdQueue:                       make(chan *DeviceCommand, commandQueueLength),
		evtQueue:                       make(chan *DeviceEvent, eventQueueLength),
		bulkDataQueue:                  make(chan *DeviceBulkData, bulkDataQueueLength),
		syncedBulkDataCh:               make(chan *DeviceSyncedBulkData, 1),
		bulkDataRequestQueue:           make(chan *DeviceBulkDataRequest, bulkDataRequestQueueLength),
		subscribeBulkDataResponseQueue: make(chan *DeviceSubscribeBulkDataResponse, subscribeBulkDataResponseQueueLength),
		bulkDataResponseQueue:          make(chan *DeviceBulkDataResponse, bulkDataResponseQueueLength),
		kvCache:                        newKeyValueCache(dc),
	}

	sess.startCommandProcessor()
	sess.startBulkDataResponseProcessor()
	go sess.kvCache.run()

	logInfo("[Session] opening MQTT connection...")
	conn, err := dc.openMQTTConn(
		sess.cmdQueue,
		sess.evtQueue,
		sess.bulkDataQueue,
		sess.syncedBulkDataCh,
		sess.bulkDataRequestQueue,
		sess.bulkDataResponseQueue,
		sess.subscribeBulkDataResponseQueue,
		sess.kvCache.syncQueue,
		sess.kvCache.pushQueue)

	if err != nil {
		return err
	}

	sess.conn = conn
	return nil
}

func terminateSession() {
	if sess != nil {
		sess.terminate()
		sess = nil
	}
}

func (s *session) terminate() {
	s.disconnect()

	if s.cmdQueue != nil {
		close(s.cmdQueue)
		s.cmdQueue = nil
	}

	if s.evtQueue != nil {
		close(s.evtQueue)
		s.evtQueue = nil
	}

	s.kvCache.terminate()
}

func (s *session) disconnect() {
	if s.conn != nil {
		s.conn.close()
		s.conn = nil
	}

	if n := len(s.evtQueue); n > 0 {
		logInfo("[Session] pending events in queue: %d", n)
	}

	if n := len(s.kvCache.pushQueue); n > 0 {
		logInfo("[Session] pending key-value updates in queue: %d", n)
	}
}

func (s *session) reconnect() error {
	if s.conn != nil {
		return errors.New("already connected")
	}

	logInfo("[Session] opening MQTT connection...")
	conn, err := s.dc.openMQTTConn(
		s.cmdQueue,
		s.evtQueue,
		s.bulkDataQueue,
		s.syncedBulkDataCh,
		s.bulkDataRequestQueue,
		s.bulkDataResponseQueue,
		s.subscribeBulkDataResponseQueue,
		s.kvCache.syncQueue,
		s.kvCache.pushQueue)

	if err != nil {
		return err
	}

	s.conn = conn
	return nil
}

func (s *session) startCommandProcessor() {
	go func() {
		logInfo("[Session] command processor is running")
		defer logInfo("[Session] command processor is exiting")

		for cmd := range s.cmdQueue {
			h, exists := commandHandlers[strings.ToLower(cmd.Action)]
			if exists {
				h(s.dc, cmd)
			} else if defaultCommandHandler != nil {
				defaultCommandHandler(s.dc, cmd)
			}
		}
	}()
}

func (s *session) startBulkDataResponseProcessor() {
	go func() {
		logInfo("[Session] BulkData Response processor is running")
		defer logInfo("[Session] BulkData Response processor is exiting")

		for response := range s.bulkDataResponseQueue {
			if receiver, ok := bulkDataResponseReceivers[strings.ToLower(response.StreamID)]; ok {
				receiver(s.dc, response)
			}
		}
	}()
}

func handleKVAccess(c *sessCtrlAccessKV, allowReadOnly bool) {
	switch c.action {
	case kvActionGet:
		if kv, err := sess.kvCache.getKeyValue(c.key); err == nil {
			c.responseKV <- kv
		} else {
			c.responseErr <- err
		}

	case kvActionGetAll:
		if kvs, err := sess.kvCache.getAllKeyValues(); err == nil {
			for _, kv := range kvs {
				c.responseKV <- kv
			}
			close(c.responseKV)
		} else {
			c.responseErr <- err
		}

	case kvActionSet:
		if allowReadOnly {
			c.responseErr <- ErrorSessionRecovering
		} else if err := sess.kvCache.setKeyValue(c.key, c.value); err == nil {
			c.responseErr <- nil
		} else {
			c.responseErr <- err
		}

	case kvActionDelete:
		if allowReadOnly {
			c.responseErr <- ErrorSessionRecovering
		} else if err := sess.kvCache.deleteKeyValue(c.key); err == nil {
			c.responseErr <- nil
		} else {
			c.responseErr <- err
		}
	}
}

func sessionIdleLoop() {
	logInfo("[SessionManager] entering idle loop")
	defer logInfo("[SessionManager] exiting idle loop")

	for {
		select {
		case c := <-sessCtrl.start:
			if err := initSession(c.dc); err == nil {
				sessState = SessionActive
			} else {
				logError("[SessionManager] connection error: %s", err.Error())
				sessState = SessionRecovering
			}

			c.response <- nil
			return

		case c := <-sessCtrl.stop:
			c.response <- ErrorSessionNotStarted

		case c := <-sessCtrl.getState:
			c.response <- SessionIdle

		case c := <-sessCtrl.accessKV:
			c.responseErr <- ErrorSessionNotStarted

		case c := <-sessCtrl.sendEvent:
			c.response <- ErrorSessionNotStarted

		case c := <-sessCtrl.sendBulkData:
			c.response <- ErrorSessionNotStarted

		case c := <-sessCtrl.writeBulkData:
			c.response <- ErrorSessionNotStarted

		case c := <-sessCtrl.sendBulkDataRequest:
			c.response <- ErrorSessionNotStarted

		case c := <-sessCtrl.subscribeBulkDataResponse:
			c.response <- ErrorSessionNotStarted
		}
	}
}

func sessionActiveLoop() {
	logInfo("[SessionManager] entering active loop")
	defer logInfo("[SessionManager] exiting active loop")

	go resubscribeBulkDataResponse()

	for {
		select {
		case c := <-sessCtrl.start:
			c.response <- ErrorSessionAlreadyStarted

		case c := <-sessCtrl.stop:
			terminateSession()
			sessState = SessionIdle
			c.response <- nil
			return

		case c := <-sessCtrl.getState:
			c.response <- SessionActive

		case err := <-sess.conn.getErrorChan():
			logError("[SessionManager] connection error: %s", err.Error())
			sess.disconnect()
			sessState = SessionRecovering
			return

		case c := <-sessCtrl.sendEvent:
			sess.evtQueue <- c.event
			c.response <- nil

		case c := <-sessCtrl.sendBulkData:
			sess.bulkDataQueue <- c.event
			c.response <- nil

		case c := <-sessCtrl.writeBulkData:
			sess.syncedBulkDataCh <- c.event

		case c := <-sessCtrl.sendBulkDataRequest:
			sess.bulkDataRequestQueue <- c.request
			c.response <- nil

		case c := <-sessCtrl.subscribeBulkDataResponse:
			sess.subscribeBulkDataResponseQueue <- c.subscribe
			c.response <- nil

		case c := <-sessCtrl.accessKV:
			handleKVAccess(c, false)

		case <-sessCtrl.ping:
			sess.conn.ping(sessPingTimeout)
		}
	}

}

func sessionRecoveringLoop() {
	logInfo("[SessionManager] entering recovery loop")
	defer logInfo("[SessionManager] exiting recovery loop")

	retry := make(chan bool, 1)
	delay := time.Second
	retryTimer := func(d time.Duration, r chan<- bool) {
		logInfo("[SessionManager] will retry in %v", d)
		time.Sleep(d)
		r <- true
	}

	go retryTimer(delay, retry)

	for {
		select {
		case <-retry:
			if err := sess.reconnect(); err == nil {
				sessState = SessionActive
				return
			}

			if delay < sessMaxRetryInterval {
				delay *= 2
			}

			go retryTimer(delay, retry)

		case c := <-sessCtrl.start:
			c.response <- ErrorSessionAlreadyStarted

		case c := <-sessCtrl.stop:
			terminateSession()
			sessState = SessionIdle
			c.response <- nil
			return

		case c := <-sessCtrl.getState:
			c.response <- SessionRecovering

		case c := <-sessCtrl.sendEvent:
			c.response <- ErrorSessionRecovering

		case c := <-sessCtrl.sendBulkData:
			c.response <- ErrorSessionRecovering

		case c := <-sessCtrl.writeBulkData:
			c.response <- ErrorSessionRecovering

		case c := <-sessCtrl.sendBulkDataRequest:
			c.response <- ErrorSessionRecovering

		case c := <-sessCtrl.subscribeBulkDataResponse:
			c.response <- ErrorSessionRecovering

		case c := <-sessCtrl.accessKV:
			handleKVAccess(c, true)
		}
	}
}

func runSessionManager() {
	logInfo("[SessionManager] starting...")

	for {
		switch sessState {
		case SessionIdle:
			sessionIdleLoop()

		case SessionActive:
			sessionActiveLoop()

		case SessionRecovering:
			sessionRecoveringLoop()
		}

		if sessStateCallback != nil {
			go sessStateCallback(sessState)
		}
	}
}

// StartSession starts a device connection session for the specified device.
// You can only have one session at a time. If you call StartSession again
// after a session has started, an error will be returned.
func StartSession(dc *DeviceContext) error {
	ctrl := &sessCtrlStart{
		dc:       dc,
		response: make(chan error, 1),
	}

	sessCtrl.start <- ctrl
	return <-ctrl.response
}

// StopSession terminates any connection session that is currently in progress.
// (in "active" or "recovering" state). It returns an error if the session is
// currently in "idle" state.
func StopSession() error {
	ctrl := &sessCtrlStop{
		response: make(chan error, 1),
	}

	sessCtrl.stop <- ctrl
	return <-ctrl.response
}

// GetSessionState returns the current state of the device's connection session.
func GetSessionState() SessionState {
	ctrl := &sessCtrlGetState{
		response: make(chan SessionState, 1),
	}

	sessCtrl.getState <- ctrl
	return <-ctrl.response
}

// SendEvent queues up a device event to be delivered to the MODE cloud.
// It returns an error if the device connection session is in idle or recovery state.
func SendEvent(eventType string, eventData map[string]interface{}, qos QOSLevel) error {
	ctrl := &sessCtrlSendEvent{
		event:    &DeviceEvent{EventType: eventType, EventData: eventData, qos: qos},
		response: make(chan error, 1),
	}

	sessCtrl.sendEvent <- ctrl
	return <-ctrl.response
}

// SendBulkData queues up a device BulkData to be delivered to the MODE cloud.
// It returns an error if the device connection session is in idle or recovery state.
func SendBulkData(streamID string, blob []byte, qos QOSLevel) error {
	ctrl := &sessCtrlSendBulkData{
		event:    &DeviceBulkData{StreamID: streamID, Blob: blob, qos: qos},
		response: make(chan error, 1),
	}

	sessCtrl.sendBulkData <- ctrl
	err := <-ctrl.response
	return err
}

// WriteBulkData is similar to SendBulkData, but instead of queueing up the data,
// this funciton will block until the data has been successfully sent at least once (QoS1).
// It returns an error if the device connection session is in idle or recovery state,
// or if the data cannot be sent after several retries.
func WriteBulkData(streamID string, blob []byte) error {
	response := make(chan error, 1)
	ctrl := &sessCtrlWriteBulkData{
		event:    &DeviceSyncedBulkData{StreamID: streamID, Blob: blob, response: response}, // Use qos only QOSAtLeastOnce
		response: response,
	}

	sessCtrl.writeBulkData <- ctrl
	err := <-ctrl.response
	return err
}

// SendBulkDataRequest queues up a device bulkDataEvent to be delivered to the MODE cloud.
// It returns an error if the device connection session is in idle or recovery state.
func SendBulkDataRequest(streamID string, requestID string, payload map[string]interface{}, qos QOSLevel) error {
	ctrl := &sessCtrlSendBulkDataRequest{
		request:  &DeviceBulkDataRequest{StreamID: streamID, RequestID: requestID, Payload: payload, qos: qos},
		response: make(chan error, 1),
	}

	sessCtrl.sendBulkDataRequest <- ctrl
	return <-ctrl.response
}

// GetKeyValue looks up a key-value pair from the Device Data Proxy.
// It returns an error if the device connection session is in idle state.
// When the session is in recovery state, the key-value pair returned will
// contain the last known value in the local memory cache.
//
// Note: Functions that access the Device Data Proxy should be called only after
// the local cache has been loaded. To get notified of this event, use
// SetKeyValuesReadyCallback() to assign a callback function.
func GetKeyValue(key string) (*KeyValue, error) {
	ctrl := &sessCtrlAccessKV{
		action:      kvActionGet,
		key:         key,
		responseErr: make(chan error, 1),
		responseKV:  make(chan *KeyValue, 1),
	}

	sessCtrl.accessKV <- ctrl

	select {
	case err := <-ctrl.responseErr:
		return nil, err
	case kv := <-ctrl.responseKV:
		return kv, nil
	}
}

// GetAllKeyValues returns all key-value pairs stored in the Device Data Proxy.
// It returns an error if the device connection session is in idle state.
// When the session is in recovery state, the key-value pairs returned will
// contain the last known values in the local memory cache.
//
// Note: Functions that access the Device Data Proxy should be called only after
// the local cache has been loaded. To get notified of this event, use
// SetKeyValuesReadyCallback() to assign a callback function.
func GetAllKeyValues() ([]*KeyValue, error) {
	ctrl := &sessCtrlAccessKV{
		action:      kvActionGetAll,
		responseErr: make(chan error, 1),
		responseKV:  make(chan *KeyValue, 1),
	}

	sessCtrl.accessKV <- ctrl
	res := make([]*KeyValue, 0, 10)

	for {
		select {
		case err := <-ctrl.responseErr:
			return nil, err
		case kv := <-ctrl.responseKV:
			if kv == nil {
				return res, nil
			}

			res = append(res, kv)
		}
	}
}

// SetKeyValue stores a key-value pair to the Device Data Proxy.
// It returns an error if the device connection session is in idle or recovery state.
//
// IMPORTANT: Race condition may arise if both the device and someone else are
// updating the same key-value pair.
//
// Note: Functions that access the Device Data Proxy should be called only after
// the local cache has been loaded. To get notified of this event, use
// SetKeyValuesReadyCallback() to assign a callback function.
func SetKeyValue(key string, value interface{}) error {
	ctrl := &sessCtrlAccessKV{
		action:      kvActionSet,
		key:         key,
		value:       value,
		responseErr: make(chan error, 1),
	}

	sessCtrl.accessKV <- ctrl
	return <-ctrl.responseErr
}

// DeleteKeyValue deletes a key-value pair from the Device Data Proxy.
// It returns an error if the device connection session is in idle or recovery state.
//
// IMPORTANT: Race condition may arise if both the device and someone else are
// updating the same key-value pair.
//
// Note: Functions that access the Device Data Proxy should be called only after
// the local cache has been loaded. To get notified of this event, use
// SetKeyValuesReadyCallback() to assign a callback function.
func DeleteKeyValue(key string) error {
	ctrl := &sessCtrlAccessKV{
		action:      kvActionDelete,
		key:         key,
		responseErr: make(chan error, 1),
	}

	sessCtrl.accessKV <- ctrl
	return <-ctrl.responseErr
}

func (s SessionState) String() string {
	switch s {
	case SessionIdle:
		return "IDLE"
	case SessionActive:
		return "ACTIVE"
	case SessionRecovering:
		return "RECOVERING"
	default:
		return "UNKNOWN"
	}
}
