package main

import (
	"fmt"
	"time"

	mode "github.com/moderepo/device-sdk-go"
)

func initDevice() {
	dc := &mode.DeviceContext{
		DeviceID:  1460,
		AuthToken: "v1.ZHwxNDYw.1478030094.51d1fc621a9c40addade401a4eb5f9d9b05054c171a62035e0d0750666f2c9ec4dbe841fd1ee2c8b733b9e1d69acfffa5c3e448c7567c9c64a67870b017f289efefc11495ec9e20c",
	}
	if err := mode.StartSession(dc, true); err != nil {
		panic(err)
	}
}

func main() {
	initDevice()

	time.Sleep(3 * time.Second)
	mode.DeleteKeyValue("hoge", map[string]interface{}{"a": 1})
	fmt.Println("vim-go")
	time.Sleep(3 * time.Second)
}
