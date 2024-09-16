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

	"github.com/moderepo/device-sdk-go/v4/mode"
)

var (
	modeMqttHost = "xxxx.corp.tinkermode.com"
	modeMqttPort = 8883
)

const (
	operationTimeout = 5 * time.Second
)

func runPingLoop(ctx context.Context, wg *sync.WaitGroup, client *mode.MqttClient) {
	// Ping on an interval (if necessary)
	pingTimer := time.NewTicker(2 * operationTimeout)
	fmt.Println("[TLS Echo] start pinger loop.")

	go func() {
		defer wg.Done()
		defer fmt.Println("[TLS Echo] stop pinger loop.")
		defer pingTimer.Stop()
		for {
			pingCtx, pingCancel := context.WithTimeout(ctx, operationTimeout)
			if err := client.PingAndWait(pingCtx); err != nil {
				fmt.Printf("[TLS Echo] Failed ping %v\n", err)
			}
			pingCancel()
			select {
			case <-pingCtx.Done():
			case <-pingTimer.C:
			case <-ctx.Done():
				return
			}

		}
	}()
}

func waitForAck(ctx context.Context, delegate *mode.ModeMqttDelegate) uint16 {
	// Block on the return channel or timeout
	select {
	case queueResp := <-delegate.QueueAckCh:
		if queueResp.Err != nil {
			fmt.Printf("[TLS Echo] Queued request failed: %s\n", queueResp.Err)
		} else {
			return queueResp.PacketID
		}
	case <-ctx.Done():
		fmt.Printf("[TLS Echo] Ack response timeout: %s\n", ctx.Err())
	}

	return 0
}

func receiveCommands(ctx context.Context, delegate *mode.ModeMqttDelegate, client *mode.MqttClient) {
	cmdChannel := delegate.GetCommandChannel()
	for {
		select {
		case cmd := <-cmdChannel:
			fmt.Printf("[TLS Echo][Echo] Received command: %s\n", cmd.Action)
			switch cmd.Action {
			case "doEcho":
				var params struct {
					Msg string `json:"msg"`
				}

				if err := cmd.BindParameters(&params); err != nil {
					fmt.Printf("[TLS Echo] Failed to bind command parameters: %s\n", err.Error())
					return
				}

				event := mode.DeviceEvent{
					EventType: "echo",
					EventData: map[string]interface{}{"msg": params.Msg},
					Qos:       mode.QOSAtLeastOnce,
				}
				pubCtx, pubCancel := context.WithTimeout(ctx, operationTimeout)
				_, err := client.PublishEvent(pubCtx, event)
				if err != nil {
					fmt.Printf("[TLS Echo] Failed to send event: %s\n", err.Error())
				}
				waitForAck(pubCtx, delegate)
				fmt.Println("[TLS Echo] ACK received from echo")
				pubCancel()
			}
		case <-ctx.Done():
			break
		}
	}
}

func main() {
	var pingWg sync.WaitGroup

	dc := &mode.DeviceContext{
		DeviceID:      0000,
		AuthToken:     "",
		TLSClientAuth: true,
	}
	// Set client certificate when you set TLSClientAuth to true.
	// change this to real file name and password
	dc.SetPKCS12ClientCertificate("fixtures/client1.p12", "pwd", false)

	delegate := mode.NewModeMqttDelegate(dc)

	client := mode.NewMqttClient(modeMqttHost, modeMqttPort, mode.WithMqttDelegate(delegate))
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	if err := client.Connect(ctx); err != nil {
		fmt.Printf("[TLS Echo] Failed to connect to %s:%d\n", modeMqttHost, modeMqttPort)
		os.Exit(1)
	}
	cancel()

	loopCtx, loopCancel := context.WithCancel(context.Background())
	// Start listening for the subscriptions before we dsubscribe and listen on
	// the channel that the listener sends to
	go delegate.StartSubscriptionListener()
	go receiveCommands(loopCtx, delegate, client)
	ctx, cancel = context.WithTimeout(context.Background(), operationTimeout)
	if err := client.Subscribe(ctx, delegate.Subscriptions()); err != nil {
		fmt.Printf("[TLS Echo] failed to subscribe: %s\n", err)
	}
	cancel()

	// Run the ping loop to keep alive while we wait for the "doEcho" command
	pingWg.Add(1)
	runPingLoop(loopCtx, &pingWg, client)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	sig := <-c
	fmt.Printf("[TLS Echo] Received signal [%v]; shutting down...\n", sig)
	loopCancel()
	pingWg.Wait()
	ctx, cancel = context.WithTimeout(context.Background(), operationTimeout)
	client.Disconnect(ctx)
	cancel()
}
