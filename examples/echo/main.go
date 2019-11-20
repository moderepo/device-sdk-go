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
	// My cloudmqtt.com server)
	modeMqttHost = "staging-api.corp.tinkermode.com"
	modeMqttPort = 8883
	modeUseTLS   = true
)

func pingLoop(client *client_api.MqttClient, timeout time.Duration,
	pingRecv <-chan bool, gotDisconnected chan<- bool, doDisconnect <-chan bool,
	wg sync.WaitGroup) {

	wg.Add(1)
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

func waitForAck(delegate *client_api.ModeMqttDelegate) uint16 {
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

func receiveCommands(cmdChannel chan *client_api.DeviceCommand,
	delegate *client_api.ModeMqttDelegate, client *client_api.MqttClient) {

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

			event := client_api.DeviceEvent{
				EventType: "echo",
				EventData: map[string]interface{}{"msg": params.Msg},
				Qos:       client_api.QOSAtLeastOnce,
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

	dc := &client_api.DeviceContext{
		DeviceID:  6040,
		AuthToken: "v1.ZHw2MDQw.1569621662.113e3dd1c0d5024816a41c0264fa0a734dfe31db218da0a480ceceee9961123348dc1aaf563480108b6f438c0710bbe244972f979e2ee08164a5d9c4c8a306d0633a96bff4edaec9",
	}

	cmdQueue := make(chan *client_api.DeviceCommand, 16)
	kvSyncQueue := make(chan *client_api.KeyValueSync, 16)
	delegate := client_api.NewModeMqttDelegate(dc, cmdQueue, kvSyncQueue)

	client := client_api.NewMqttClient(modeMqttHost, modeMqttPort, nil,
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
