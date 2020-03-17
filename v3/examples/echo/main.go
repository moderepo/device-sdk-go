/*
In this example, the device sends an "echo" event whenever it receives a "doEcho"
command.
*/
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/moderepo/device-sdk-go/v3"
)

var (
	// Set these to the mode server (or start the mqtt_dummy)
	modeMqttHost = "localhost"
	modeMqttPort = 1998
	modeUseTLS   = false
)

func pingLoop(ctx context.Context, client *mode.MqttClient,
	timeout time.Duration, pingRecv <-chan mode.MqttResponse, wg *sync.WaitGroup) {

	defer func() {
		fmt.Println("Exiting ping loop")
		wg.Done()
	}()
	ticker := time.NewTicker(3 * time.Second)

	for {
		if err := client.Ping(); err != nil {
			return
		}

		// New context for timing out
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Block on the return channel or timeout
		select {
		case resp := <-pingRecv:
			// There's not really a way to return false, but, since it's bool,
			// we'll check
			fmt.Println("Got ping response")
			if resp.Err != nil {
				fmt.Println("sender closed ping channel")
				return
			}
		case <-ctx.Done():
			// If we were cancelled, wait for the ping response before quitting
			switch ctx.Err() {
			case context.Canceled:
				// We don't want the ping response to get written on an empty
				// channel, so wait for it. In a real world example, we would
				// continue the loop or create a new context, but it's a rare
				// case that we're cancelling *and* the ping response gets
				// lost.
				<-pingRecv
			case context.DeadlineExceeded:
				// timeout, so we're not going to wait for the response anymore
			}
			return
		}

		fmt.Println("Ping sent and ack'ed")
		// Wait for the timer to before we loop again
		<-ticker.C
	}
}

func waitForAck(delegate *mode.ModeMqttDelegate) uint16 {
	// Wait for the ack, or timeout
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Second)
	defer cancel()

	// Block on the return channel or timeout
	select {
	case queueResp := <-delegate.QueueAckCh:
		if queueResp.Err != nil {
			fmt.Printf("Queued request failed: %s\n", queueResp.Err)
		} else {
			return queueResp.PacketID
		}
	case <-ctx.Done():
		fmt.Printf("Ack response timeout: %s\n", ctx.Err())
	}

	return 0
}

func receiveCommands(cmdChannel chan *mode.DeviceCommand,
	delegate *mode.ModeMqttDelegate, client *mode.MqttClient) {

	for cmd := range cmdChannel {
		switch cmd.Action {
		case "doEcho":
			var params struct {
				Msg string `json:"msg"`
			}

			if err := cmd.BindParameters(&params); err != nil {
				fmt.Printf("Failed to bind command parameters: %s\n", err.Error())
				return
			}

			event := mode.DeviceEvent{
				EventType: "echo",
				EventData: map[string]interface{}{"msg": params.Msg},
				Qos:       mode.QOSAtLeastOnce,
			}

			_, err := client.PublishEvent(event)
			if err != nil {
				fmt.Printf("Failed to send event: %s\n", err.Error())
			}
			waitForAck(delegate)
			fmt.Println("ACK received from echo")
		}
	}
}

func main() {
	var pingWg sync.WaitGroup

	dc := &mode.DeviceContext{
		DeviceID:  1234,
		AuthToken: "v1.XXXXXXXX",
	}

	cmdQueue := make(chan *mode.DeviceCommand, 16)
	kvSyncQueue := make(chan *mode.KeyValueSync, 16)
	delegate := mode.NewModeMqttDelegate(dc, cmdQueue, kvSyncQueue)

	client := mode.NewMqttClient(modeMqttHost, modeMqttPort, delegate)
	if err := client.Connect(); err != nil {
		fmt.Printf("Failed to connect to %s:%d\n", modeMqttHost, modeMqttPort)
		os.Exit(1)
	}

	// Start listening for the subscriptions before we subscribe and listen on
	// the channel that the listener sends to
	go delegate.RunSubscriptionListener()
	go receiveCommands(cmdQueue, delegate, client)
	if err := client.Subscribe(); err != nil {
		fmt.Printf("failed to subscribe: %s\n", err)
	}

	pingCtx, pingCancel := context.WithCancel(context.Background())

	// Run the ping loop to keep alive while we wait for the "doEcho" command
	pingWg.Add(1)
	go pingLoop(pingCtx, client, 5*time.Second, delegate.PingAckCh, &pingWg)

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	sig := <-c

	fmt.Printf("Received signal [%v]; shutting down...\n", sig)
	pingCancel()
	pingWg.Wait()
	client.Disconnect()
}
