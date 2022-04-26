package websocket

import (
	"bytes"
	"fmt"
	"net"
	"time"

	"github.com/gorilla/websocket"
)

type (
	// Conn is a wrapper struct for WebSocket connection.
	Conn struct {
		*websocket.Conn
		readBuffer bytes.Buffer
	}
)

var _ net.Conn = (*Conn)(nil)

// Read message from WebSocket.
func (c Conn) Read(b []byte) (n int, err error) {
	// If there is remaining in the buffer, return it first.
	if c.readBuffer.Len() > 0 {
		return c.readBuffer.Read(b)
	}

	// Read buffer is empty. Read from the underlying connection.
	// If the broker is idle and no data is ready, ReadMessage will return timeout error after the deadline.
	// Gorilla's SetReadDeadline is different from net.Conn's SetReadDeadline.
	// After a read has timed out, the websocket connection state is corrupt and all future reads will return an error.
	// As a workaround, I set a zero value for deadline implicitly so that reads will not time out.
	if err := c.Conn.SetReadDeadline(time.Time{}); err != nil {
		return 0, err
	}
	_, reader, err := c.Conn.NextReader()
	if err != nil {
		return 0, err
	}

	// Buffer the message (=MQTT packet) because the reader may not have enough read buffer capacity.
	if _, err := c.readBuffer.ReadFrom(reader); err != nil {
		return 0, err
	}

	return c.readBuffer.Read(b)
}

// Write message to WebSocket.
func (c Conn) Write(b []byte) (n int, err error) {
	if err := c.Conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// SetDeadline both read and write.
func (c Conn) SetDeadline(t time.Time) error {
	if err := c.Conn.SetReadDeadline(t); err != nil {
		return err
	}
	if err := c.Conn.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

// Dial creates a new WebSocket connection.
func Dial(network, addr string) (net.Conn, error) {
	urlStr := fmt.Sprintf("%s://%s", network, addr)
	c, _, err := websocket.DefaultDialer.Dial(urlStr, nil)
	if err != nil {
		return nil, err
	}

	return &Conn{Conn: c}, nil
}
