# MODE Device SDK for Go

[![GoDoc](https://godoc.org/github.com/moderepo/device-sdk-go?status.svg)](https://godoc.org/github.com/moderepo/device-sdk-go)

This SDK is for anyone implementing MODE device drivers in the Go language. It is
being released as a public Go module that provides a Go API for devices to interact with the MODE cloud.


## Installation

Our recommendation is to use the package as a module. Go will automatically
install the package when it sees that it is required, but if you haven't
initialized your package as a module to consume the the module:

    $ go mod init <your package>

This will allow go you to download and install the module at build or run time.

If you are not yet using go modules and are still in GOPATH mode, you may use
'go get' to fetch the SDK:

    $ go get github.com/moderepo/device-sdk-go

## Using the SDK

The default package name is `mode`. A trivial example:

    package main

    import (
        "fmt"
        "github.com/moderepo/device-sdk-go"
    )

    func main() {
        dc := &mode.DeviceContext{
            DeviceID:  __DEVICE_ID__,
            AuthToken: "__DEVICE_TOKEN__",
        }

        if d, err := dc.GetInfo(); err == nil {
            fmt.Printf("I am %v\n", d)
        }
    }


See more examples [here](https://github.com/moderepo/device-sdk-go/blob/master/examples).


## Documentation

See the full API documentation [here](https://godoc.org/github.com/moderepo/device-sdk-go).


## Copyright and License

Code and documentation copyright 2017 Mode, Inc. Released under the [MIT license](https://github.com/moderepo/device-sdk-go/blob/master/LICENSE).
