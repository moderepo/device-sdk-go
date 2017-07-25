package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	mode "github.com/moderepo/device-sdk-go"
)

func initDevice() {
	dc := &mode.DeviceContext{
		DeviceID:  0,
		AuthToken: "",
	}
	if err := mode.StartSession(dc, true); err != nil {
		panic(err)
	}
}

func main() {
	initDevice()

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	sig := <-c
	fmt.Printf("Signal %d\n", sig)
}
