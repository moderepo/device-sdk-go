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
		DeviceID:  0,             // change this to real device ID
		AuthToken: "XXXXXXXXXXX", // change this to real API key assigned to device
	}

	mode.SetKeyValuesReadyCallback(func(_ *mode.DeviceContext) {
		if kvs, err := mode.GetAllKeyValues(); err == nil {
			fmt.Printf("Key-value pairs:\n")
			for i, kv := range kvs {
				fmt.Printf("  %d: %v\n", i, kv)
			}
		}
	})

	mode.SetKeyValueStoredCallback(func(_ *mode.DeviceContext, kv *mode.KeyValue) {
		// If the key 'msg' is updated, echo it back to the key 'echo'.
		if kv.Key == "msg" {
			fmt.Printf("Echoing msg %v\n", kv.Value)

			if err := mode.SetKeyValue("echo", kv.Value); err != nil {
				fmt.Printf("Failed to set key-value: %s\n", err.Error())
			}
		}
	})

	// Start connection session using MQTT.
	if err := mode.StartSession(dc, true); err != nil {
		fmt.Printf("Failed to start session: %s\n", err.Error())
		os.Exit(1)
	}

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	sig := <-c
	fmt.Printf("Received signal [%v]; shutting down...\n", sig)
	mode.StopSession()
}
