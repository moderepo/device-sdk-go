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

	"github.com/moderepo/device-sdk-go/v2"
)

var (
	modeMqttHost = "staging-api.corp.tinkermode.com"
	modeMqttPort = 8883
	modeUseTLS   = true
)

// XXX - Go back and sync this code up with the regular echo when the code stabilizes
func pingLoop(client *mode.MqttClient, timeout time.Duration,
	pingRecv <-chan bool, gotDisconnected chan<- bool, doDisconnect <-chan bool,
	wg sync.WaitGroup) {

	defer func() {
		fmt.Println("Exiting ping loop")
		wg.Done()
	}()
	ticker := time.NewTicker(3 * time.Second)

	for {
		if err := client.Ping(); err != nil {
			gotDisconnected <- true
			return
		}

		// Wait for the ack, or timeout
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Block on the return channel or timeout
		select {
		case <-doDisconnect:
			fmt.Println("runner told me to exit")
			return
		case ret := <-pingRecv:
			// There's not really a way to return false, but, since it's bool,
			// we'll check
			fmt.Println("Got ping response")
			if !ret {
				fmt.Println("sender closed ping channel")
				gotDisconnected <- true
			}
		case <-ctx.Done():
			fmt.Println("ping timeout")
			gotDisconnected <- true
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
	case queueRes := <-delegate.QueueAckCh:
		if queueRes.Err != nil {
			fmt.Printf("Queued request failed: %s\n", queueRes.Err)
		} else {
			return queueRes.PacketId
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
		DeviceID:  0000,
		AuthToken: "v1.XXXXXXXX"
		TLSClientAuth: true,
	}
	// Set client certificate when you set TLSClientAuth to true.
	// change this to real file name and password
	dc.SetPKCS12ClientCertificate("fixtures/client1.p12", "pwd", true)

	cmdQueue := make(chan *mode.DeviceCommand, 16)
	kvSyncQueue := make(chan *mode.KeyValueSync, 16)
	delegate := mode.NewModeMqttDelegate(dc, cmdQueue, kvSyncQueue)

	client := mode.NewMqttClient(modeMqttHost, modeMqttPort, nil,
		modeUseTLS, delegate)
	if err := client.Connect(); err != nil {
		fmt.Printf("Failed to connect to %s:%d\n", modeMqttHost, modeMqttPort)
		os.Exit(1)
	}

	// Start listening for the subscriptions before we subscribe and listen on
	// the channel that the listener sends to
	go delegate.RunSubscriptionListener()
	go receiveCommands(cmdQueue, &delegate, client)
	if err := client.Subscribe(); err != nil {
		fmt.Printf("failed to subscribe: %s\n", err)
	}

	stopPingCh := make(chan bool)
	pingFailCh := make(chan bool)

	// Run the ping loop to keep alive while we wait for the "doEcho" command
	pingWg.Add(1)
	go pingLoop(client, 5*time.Second, delegate.PingAckCh, pingFailCh,
		stopPingCh, pingWg)

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-c:
		fmt.Printf("Received signal [%v]; shutting down...\n", sig)
	case <-pingFailCh:
		fmt.Printf("Ping failed. shutting down...\n")
	}
	close(stopPingCh)
	pingWg.Wait()
	client.Disconnect()
}
