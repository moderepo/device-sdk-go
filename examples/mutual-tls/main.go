/*
This is an example of how a device can communicate with the MODE
cloud via Device Data Proxy.
*/
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	mode "github.com/moderepo/device-sdk-go"
)

func main() {
	dc := &mode.DeviceContext{
		TLSClientAuth:  true,
		DeviceID:       0,                    // change this to real device ID
		PKCS12FileName: "/etc/xxxxxssss.p12", // change this to PKCS#12 file path
		PKCS12Password: "xxxxxxxxxx",         // change this to PKCS#12 password
	}

	// Default MQTT host (mqtt.tinkermode.com) doesn't support TLS Client Authentication.
	// You need to set MQTT host manually.
	mqttHost := "xxxxxxx.corp.tinkermode.com" // change this to real mqtt host
	mqttPort := 1883                          // change this to real mqtt port
	mode.SetMQTTHostPort(mqttHost, mqttPort, true)

	mode.SetKeyValuesReadyCallback(func(_ *mode.DeviceContext) {
		if kvs, err := mode.GetAllKeyValues(); err == nil {
			fmt.Printf("Key-value pairs:\n")
			for i, kv := range kvs {
				fmt.Printf("  %d: %v\n", i, kv)
			}
		}
	})

	mode.SetKeyValueStoredCallback(func(_ *mode.DeviceContext, kv *mode.KeyValue) {
		// If the key 'msg' is updated, emit a device event.
		if kv.Key == "msg" {
			eventData := map[string]interface{}{"msg": kv.Value}

			if err := mode.SendEvent("msgUpdated", eventData, mode.QOSAtLeastOnce); err != nil {
				fmt.Printf("Failed to send event: %v\n", err)
			}
		}
	})

	if err := mode.StartSession(dc); err != nil {
		fmt.Printf("Failed to start session: %v\n", err)
		os.Exit(1)
	}

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	sig := <-c
	fmt.Printf("Received signal [%v]; shutting down...\n", sig)
	mode.StopSession()
}
