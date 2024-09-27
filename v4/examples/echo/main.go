/*
In this example, the device sends an "echo" event whenever it receives a "doEcho"
command.
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/moderepo/device-sdk-go/v4/mode"
)

const (
	defaultMQTTHost  = "mqtt.tinkermode.com"
	defaultMQTTPort  = 1883
	defaultMQTTSPort = 8883
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
	var modeMqttHost string
	var useTLS bool

	flag.StringVar(&modeMqttHost, "h", defaultMQTTHost, "hostname of MQTT server")
	flag.BoolVar(&useTLS, "s", false, "use secure connection")
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("Usage: echo <device_id> <auth_token>")
		os.Exit(1)
	}

	deviceID, err := strconv.ParseUint(flag.Arg(0), 10, 64)
	if err != nil || deviceID == 0 {
		fmt.Printf("Invalid device id: %s\n", flag.Arg(0))
		os.Exit(1)
	}

	authToken := strings.TrimSpace(flag.Arg(1))
	if len(authToken) == 0 {
		fmt.Printf("Auth token required\n")
		os.Exit(1)
	}

	dc := &mode.DeviceContext{
		DeviceID:  deviceID,
		AuthToken: authToken,
	}

	delegate := mode.NewModeMqttDelegate(dc, mode.WithUseTLS(useTLS))

	var modeMqttPort int
	if useTLS {
		modeMqttPort = defaultMQTTSPort
	} else {
		modeMqttPort = defaultMQTTPort
	}

	client := mode.NewMqttClient(modeMqttHost, modeMqttPort, mode.WithMqttDelegate(delegate))

	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		fmt.Printf("[Echo] Failed to connect to %s:%d\n", modeMqttHost, modeMqttPort)
		os.Exit(1)
	}

	loopCtx, loopCancel := context.WithCancel(context.Background())
	// Start listening for the subscriptions before we subscribe and listen on
	// the channel that the listener sends to
	go delegate.StartSubscriptionListener()
	go receiveCommands(loopCtx, delegate, client)

	ctx, cancel = context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	if err := client.Subscribe(ctx, delegate.Subscriptions()); err != nil {
		fmt.Printf("[Echo] failed to subscribe: %s\n", err)
	}

	var pingWg sync.WaitGroup
	// Run the ping loop to keep alive while we wait for the "doEcho" command
	pingWg.Add(1)
	runPingLoop(loopCtx, &pingWg, client)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	sig := <-c
	fmt.Printf("[Echo] Received signal [%v]; shutting down...\n", sig)
	loopCancel()
	pingWg.Wait()

	ctx, cancel = context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	_ = client.Disconnect(ctx)
}
