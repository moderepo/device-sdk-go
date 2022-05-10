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
	// Set these to the mode server
	modeRestHost = "localhost"
	modeRestPort = 7002

	modeMqttHost    = "localhost"
	modeMqttWSPort  = 21883
	modeMqttWSSPort = 28883
)

const (
	operationTimeout = 5 * time.Second
)

func runPingLoop(ctx context.Context, wg *sync.WaitGroup, client *mode.MqttClient) {
	// Ping on an interval (if necessary)
	pingTimer := time.NewTicker(2 * operationTimeout)
	fmt.Println("[Echo] start pinger loop.")

	go func() {
		defer wg.Done()
		defer fmt.Println("[Echo] stop pinger loop.")
		defer pingTimer.Stop()
		for {
			pingCtx, pingCancel := context.WithTimeout(ctx, operationTimeout)
			if err := client.PingAndWait(pingCtx); err != nil {
				fmt.Printf("[Echo] Failed ping %v\n", err)
			}
			pingCancel()
			select {
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
			fmt.Printf("Queued request failed: %s\n", queueResp.Err)
		} else {
			return queueResp.PacketID
		}
	case <-ctx.Done():
		fmt.Printf("Ack response timeout: %s\n", ctx.Err())
	}

	return 0
}

func receiveCommands(ctx context.Context, delegate *mode.ModeMqttDelegate, client *mode.MqttClient) {
	cmdChannel := delegate.GetCommandChannel()
	for {
		select {
		case cmd := <-cmdChannel:
			fmt.Printf("[Echo] Received command: %s\n", cmd.Action)
			switch cmd.Action {
			case "doEcho":
				var params struct {
					Msg string `json:"msg"`
				}

				if err := cmd.BindParameters(&params); err != nil {
					fmt.Printf("[Echo] Failed to bind command parameters: %s\n", err.Error())
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
					fmt.Printf("[Echo] Failed to send event: %s\n", err.Error())
				}
				waitForAck(pubCtx, delegate)
				fmt.Println("[Echo] ACK received from echo")
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
		DeviceID:  0000,
		AuthToken: "v1.xxxxxxxx",
	}

	mode.SetRESTHostPort(modeRestHost, modeRestPort, true)
	if info, err := dc.GetInfo(); err != nil {
		fmt.Printf("[Echo] Failed to get device info: %v\n", err)
		os.Exit(1)
	} else {
		fmt.Printf("[Echo] Device info: %v\n", info)
	}

	delegate := mode.NewModeMqttDelegate(dc)

	// WS example
	delegate.UseTLS = false
	port := modeMqttWSPort

	//// WSS example
	//delegate.UseTLS = true
	//port := modeMqttWSSPort

	//// WSS with client certificate example
	//// A client certificate must have device ID in the common name. The common name format is `device-[0-9]*` (e.g. device-123).
	//// Also, PKCS12 file should be base64 encoded (e.g. openssl enc -e -base64 -in client.p12 -out client.b64).
	//port := modeMqttWSSPort
	//dc = &mode.DeviceContext{} // Since client certificate contains device ID, we don't need to set device ID to device context.
	//delegate = mode.NewModeMqttDelegate(dc)
	//if err := dc.SetPKCS12ClientCertificate("client.b64", "pwd", false); err != nil {
	//	fmt.Printf("[Echo] Failed to set client certificate: %v\n", err)
	//	os.Exit(1)
	//}

	client := mode.NewMqttClient(modeMqttHost, port, mode.WithMqttDelegate(delegate), mode.WithUseWebSocket(true))
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	if err := client.Connect(ctx); err != nil {
		fmt.Printf("[Echo] Failed to connect to %s:%d\n", modeMqttHost, port)
		os.Exit(1)
	}
	cancel()

	loopCtx, loopCancel := context.WithCancel(context.Background())
	// Start listening for the subscriptions before we subscribe and listen on
	// the channel that the listener sends to
	go delegate.StartSubscriptionListener()
	go receiveCommands(loopCtx, delegate, client)
	ctx, cancel = context.WithTimeout(context.Background(), operationTimeout)
	if err := client.Subscribe(ctx, delegate.Subscriptions()); err != nil {
		fmt.Printf("[Echo] failed to subscribe: %s\n", err)
	}
	cancel()

	// Run the ping loop to keep alive while we wait for the "doEcho" command
	pingWg.Add(1)
	runPingLoop(loopCtx, &pingWg, client)

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	sig := <-c
	fmt.Printf("[Echo] Received signal [%v]; shutting down...\n", sig)
	loopCancel()
	pingWg.Wait()
	ctx, cancel = context.WithTimeout(context.Background(), operationTimeout)
	client.Disconnect(ctx)
	cancel()
}
