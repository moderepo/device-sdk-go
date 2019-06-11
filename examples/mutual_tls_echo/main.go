/*
In this example, the device sends an "echo" event whenever it receives a "doEcho"
command.
*/
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	mode "github.com/moderepo/device-sdk-go"
)

func doEcho(_ *mode.DeviceContext, cmd *mode.DeviceCommand) {
	// Command parameters are JSON encoded and can be retrieved as follows.
	var params struct {
		Msg string `json:"msg"`
	}

	if err := cmd.BindParameters(&params); err != nil {
		fmt.Printf("Failed to bind command parameters: %s\n", err.Error())
		return
	}

	eventData := map[string]interface{}{"msg": params.Msg}

	if err := mode.SendEvent("echo", eventData, mode.QOSAtLeastOnce); err != nil {
		fmt.Printf("Failed to send event: %s\n", err.Error())
	}
}

func main() {
	dc := &mode.DeviceContext{
		TLSClientAuth:      true,
		DeviceID:           0,                    // change this to real device ID
		PKCS12FileName:     "/etc/xxxxxssss.p12", // change this to PKCS#12 file path
		PKCS12Password:     "xxxxxxxxxx",         // change this to PKCS#12 password
		InsecureSkipVerify: true,                 // This should be used only for testing.
	}

	// Default REST host (api.tinkermode.com) doesn't support TLS Client Authentication.
	// You need to set REST host manually.
	restHost := "xxxxxxx.corp.tinkermode.com" // change this to real REST host
	restPort := 7002                          // change this to real REST port
	mode.SetRESTHostPort(restHost, restPort, true)

	// Default MQTT host (mqtt.tinkermode.com) doesn't support TLS Client Authentication.
	// You need to set MQTT host manually.
	mqttHost := "xxxxxxx.corp.tinkermode.com" // change this to real MQTT host
	mqttPort := 1883                          // change this to real MQTT port
	mode.SetMQTTHostPort(mqttHost, mqttPort, true)

	mode.SetCommandHandler("doEcho", doEcho)

	mode.SetDefaultCommandHandler(func(_ *mode.DeviceContext, cmd *mode.DeviceCommand) {
		fmt.Printf("Received unknown command %s\n", cmd.Action)
	})

	mode.SetSessionStateCallback(func(state mode.SessionState) {
		fmt.Printf("Session state changed to %v\n", state)
	})

	if d, err := dc.GetInfo(); err == nil {
		fmt.Printf("Running as device %v\n", d)
	} else {
		fmt.Printf("Failed to get device info: %s\n", err.Error())
		os.Exit(1)
	}

	if err := mode.StartSession(dc); err != nil {
		fmt.Printf("Failed to start session: %s\n", err.Error())
		os.Exit(1)
	}

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	sig := <-c
	fmt.Printf("Received signal [%v]; shutting down...\n", sig)
	mode.StopSession()
}
