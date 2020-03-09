# MODE Device SDK for Go

[![GoDoc](https://godoc.org/github.com/moderepo/device-sdk-go?status.svg)](https://godoc.org/github.com/moderepo/device-sdk-go)

This SDK is for anyone implementing MODE device drivers in the Go language. It is
being released as a public Go module that provides a Go API for devices to interact with the MODE cloud.


## Installation

Our recommendation is to use the package as a module. In module mode, Go will
automatically download the module when it sees an `import` statement
referencing this repository. If it is not already, your workspace should be a
Go module which will be able to consume this one:

    $ go mod init <your module path>

This will allow go you to download and install the module at build or run time.

If you are not yet using Go modules and are still in GOPATH mode, you may use
'go get' to fetch the SDK:

    $ go get github.com/moderepo/device-sdk-go

## Using the SDK

The package name is `mode` (which does not match the import path). For example:
```golang
    package main

    import (
        "fmt"
        // For version 2 of the API
        "github.com/moderepo/device-sdk-go/v2"
        // For version 3 of the API
        "github.com/moderepo/device-sdk-go/v3"
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
```

See more examples for [version 2](https://github.com/moderepo/device-sdk-go/blob/master/examples) of the API and for [version 3](https://github.com/moderepo/device-sdk-go/blob/master/v3/examples).



## Documentation

See the full API documentation
 * [Version 2](https://godoc.org/github.com/moderepo/device-sdk-go).
 * [Version 3](https://godoc.org/github.com/moderepo/device-sdk-go/v3).


## Copyright and License

Code and documentation copyright 2019 Mode, Inc. Released under the [MIT
license](https://github.com/moderepo/device-sdk-go/blob/master/LICENSE).
