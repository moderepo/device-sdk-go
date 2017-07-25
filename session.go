package mode

import (
	"errors"
	"strings"
	"time"
)

type (
	// Connection can be either websocket or MQTT connection.
	connection interface {
		close()
		getErrorChan() chan error
		ping(timeout time.Duration)
	}

	session struct {
		dc          *DeviceContext
		usingMQTT   bool
		conn        connection
		cmdQueue    chan *DeviceCommand
		evtQueue    chan *DeviceEvent
		sendKvQueue chan *KeyValue
		recvKvQueue chan *KeyValue
	}

	sessCtrlStart struct {
		dc       *DeviceContext
		useMQTT  bool
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

	sessCtrlSendKeyValue struct {
		keyValue *KeyValue
		response chan error
	}

	// SessionState represents a state of the device's connection session.
	SessionState int

	// A callback function that is invoked when the device's connection session
	// changes state.
	SessionStateCallback func(SessionState)
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
		start        chan *sessCtrlStart
		stop         chan *sessCtrlStop
		getState     chan *sessCtrlGetState
		sendEvent    chan *sessCtrlSendEvent
		sendKeyValue chan *sessCtrlSendKeyValue
		ping         <-chan time.Time
	}

	sess *session

	commandHandlers       = map[string]CommandHandler{}
	defaultCommandHandler CommandHandler

	sessStateCallback SessionStateCallback

	ErrorSessionAlreadyStarted = errors.New("session already started")
	ErrorSessionNotStarted     = errors.New("not in session")
	ErrorSessionRecovering     = errors.New("session is recovering")

	sessPingInterval = sessDefaultPingInterval
	sessPingTimeout  = sessDefaultPingTimeout

	kvReloadHandler KvReloadHandler
	kvSetHandler    KvSetHandler
	kvDeleteHandler KvDeleteHandler
	kvStore         = map[string]*KeyValue{}
)

func init() {
	sessCtrl.start = make(chan *sessCtrlStart, 1)
	sessCtrl.stop = make(chan *sessCtrlStop, 1)
	sessCtrl.getState = make(chan *sessCtrlGetState, 1)
	sessCtrl.sendEvent = make(chan *sessCtrlSendEvent, 1)
	sessCtrl.sendKeyValue = make(chan *sessCtrlSendKeyValue, 1)
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

func SetKvReloadHandler(h KvReloadHandler) {
	kvReloadHandler = h
}

func SetKvSetHandler(h KvSetHandler) {
	kvSetHandler = h
}

func SetKvDeleteHandler(h KvDeleteHandler) {
	kvDeleteHandler = h
}

// When the device's connection session is in the "active" state, "pings"
// are periodically sent to the server. A ping fails if the server doesn't
// respond. ConfigurePings overrides the default time interval between pings and
// the timeout for the server's responses to pings.
func ConfigurePings(interval time.Duration, timeout time.Duration) {
	sessPingInterval = interval
	sessPingTimeout = timeout
}

func initSession(dc *DeviceContext, useMQTT bool) error {
	sess = &session{
		dc:          dc,
		usingMQTT:   useMQTT,
		cmdQueue:    make(chan *DeviceCommand, commandQueueLength),
		evtQueue:    make(chan *DeviceEvent, eventQueueLength),
		sendKvQueue: make(chan *KeyValue, eventQueueLength),
		recvKvQueue: make(chan *KeyValue, eventQueueLength),
	}

	var conn connection
	var err error

	if useMQTT {
		logInfo("[Session] opening MQTT connection...")
		conn, err = dc.openMQTTConn(sess.cmdQueue, sess.evtQueue, sess.recvKvQueue, sess.sendKvQueue)
	} else {
		logInfo("[Session] opening websocket connection...")
		conn, err = dc.openWebsocket(sess.cmdQueue, sess.evtQueue)
	}

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

	if s.sendKvQueue != nil {
		close(s.sendKvQueue)
		s.sendKvQueue = nil
	}

	if s.recvKvQueue != nil {
		close(s.recvKvQueue)
		s.recvKvQueue = nil
	}
}

func (s *session) disconnect() {
	if s.conn != nil {
		s.conn.close()
		s.conn = nil
	}

	if n := len(s.evtQueue); n > 0 {
		logInfo("[Session] pending events in queue: %d", n)
	}

	if n := len(s.sendKvQueue); n > 0 {
		logInfo("[Session] pending key value set in queue: %d", n)
	}
}

func (s *session) reconnect() error {
	if s.conn != nil {
		return errors.New("already connected")
	}

	var conn connection
	var err error

	if s.usingMQTT {
		logInfo("[Session] opening MQTT connection...")
		conn, err = s.dc.openMQTTConn(s.cmdQueue, s.evtQueue, s.recvKvQueue, s.sendKvQueue)
	} else {
		logInfo("[Session] opening websocket connection...")
		conn, err = s.dc.openWebsocket(s.cmdQueue, s.evtQueue)
	}

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

func (s *session) kvReload(rev int, items []interface{}) bool {
	logInfo("[Session] Rev %d number of Items: %d", rev, len(items))

	// Clear kvStore
	kvStore = map[string]*KeyValue{}

	for _, i := range items {
		// TODO: Fix better casting
		item := i.(map[string]interface{})
		key := item["key"].(string)
		value := item["value"].(map[string]interface{})
		mtime, _ := time.Parse(time.RFC3339Nano, item["modificationTime"].(string))
		kv := &KeyValue{Rev: rev, Value: value, MTime: mtime}
		kvStore[key] = kv
		logInfo("[Session] key: %s", key)
	}

	return true
}

func (s *session) kvSet(rev int, key string, value map[string]interface{}) bool {
	// TODO: Need mtime?
	if len(key) == 0 {
		logError("[Session] Invalid Key")
		return false
	}

	if value == nil {
		logError("[Session] Invalid value")
		return false
	}

	stored, ok := kvStore[key]
	if !ok {
		kvStore[key] = &KeyValue{Rev: rev, Value: value, MTime: time.Now()}
		logInfo("[Session] kvSet saved new key %s", key)
		return true
	}

	if rev <= stored.Rev {
		logInfo("[Session] kvSet ignored obsolete update (rev %d) to key %s (rev %d)", key, rev, stored.Rev)
		return false
	}

	stored.Rev = rev
	stored.Value = value
	stored.MTime = time.Now()
	logInfo("[Session] kvSet updated value of key %s", key)

	return true
}

func (s *session) kvDelete(rev int, key string) bool {
	if len(key) == 0 {
		logError("[Session] Invalid Key")
		return false
	}

	stored, ok := kvStore[key]
	if !ok {
		// This can happen if SET and DELETE transactions are out of order.
		// Record the delete anyway.
		kvStore[key] = &KeyValue{Rev: rev, MTime: time.Now()}

		logInfo("[Session] kvDelete deleted value for key '%s'", key)
		return true
	}

	if rev <= stored.Rev {
		logInfo("[Session] kvDelete ignored obsolete update (rev %d) to key %s (rev %d)", key, rev, stored.Rev)
		return false
	}

	stored.Rev = rev
	stored.Value = nil
	stored.MTime = time.Now()
	logInfo("[Session] kvDelete deleted value for key %s", key)
	return true
}

func (s *session) keyValueHandler(dc *DeviceContext, kv *KeyValue) {

	switch kv.Action {
	case "reload":
		if s.kvReload(kv.Rev, kv.Items) && kvReloadHandler != nil {
			items := []map[string]interface{}{}
			for _, item := range kv.Items {
				items = append(items, item.(map[string]interface{}))
			}
			kvReloadHandler(items)
		}
	case "set":
		if s.kvSet(kv.Rev, kv.Key, kv.Value) && kvSetHandler != nil {
			kvSetHandler(kv.Key, kv.Value)
		}
	case "delete":
		if s.kvDelete(kv.Rev, kv.Key) && kvDeleteHandler != nil {
			kvDeleteHandler(kv.Key)
		}

	default:
		logError("[Session] received sync message with unknown action %s", kv.Action)
	}
}

func (s *session) startKeyValueProcessor() {
	go func() {
		logInfo("[Session] command processor is running")
		defer logInfo("[Session] command processor is exiting")

		for kv := range s.recvKvQueue {
			s.keyValueHandler(s.dc, kv)
		}
	}()
}

func sessionIdleLoop() {
	logInfo("[SessionManager] entering idle loop")
	defer logInfo("[SessionManager] exiting idle loop")

	for {
		select {
		case c := <-sessCtrl.start:
			if err := initSession(c.dc, c.useMQTT); err == nil {
				sess.startCommandProcessor()
				sess.startKeyValueProcessor()
				sessState = SessionActive
			} else {
				logError("[SessionManager] session not started: %s", err.Error())
				sessState = SessionRecovering
			}

			c.response <- nil
			return

		case c := <-sessCtrl.stop:
			c.response <- ErrorSessionNotStarted

		case c := <-sessCtrl.getState:
			c.response <- SessionIdle

		case c := <-sessCtrl.sendEvent:
			c.response <- ErrorSessionNotStarted

		case c := <-sessCtrl.sendKeyValue:
			c.response <- ErrorSessionNotStarted
		}
	}
}

func sessionActiveLoop() {
	logInfo("[SessionManager] entering active loop")
	defer logInfo("[SessionManager] exiting active loop")

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

		case c := <-sessCtrl.sendKeyValue:
			sess.sendKvQueue <- c.keyValue
			c.response <- nil

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
// By default, the connection is made using HTTP/websocket. If useMQTT is true,
// the session will connect by MQTT instead
//
// You can only have one session at a time. If you call StartSession again
// after a session has started, an error will be returned.
func StartSession(dc *DeviceContext, useMQTT bool) error {
	ctrl := &sessCtrlStart{
		dc:       dc,
		useMQTT:  useMQTT,
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

func SetKeyValue(key string, value map[string]interface{}) error {
	ctrl := &sessCtrlSendKeyValue{
		keyValue: &KeyValue{Action: "set", Key: key, Value: value},
		response: make(chan error, 1),
	}

	sessCtrl.sendKeyValue <- ctrl
	return <-ctrl.response
}

func DeleteKeyValue(key string, value map[string]interface{}) error {
	ctrl := &sessCtrlSendKeyValue{
		keyValue: &KeyValue{Action: "delete", Key: key, Value: value},
		response: make(chan error, 1),
	}

	sessCtrl.sendKeyValue <- ctrl
	return <-ctrl.response
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
