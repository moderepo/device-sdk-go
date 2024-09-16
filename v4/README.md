# MODE Device SDK for Go (version 4)

[![GoDoc](https://pkg.go.dev/github.com/moderepo/device-sdk-go/v4?status.svg)](https://pkg.go.dev/github.com/moderepo/device-sdk-go/v4)

This SDK is for anyone implementing MODE device drivers in the Go language. It is
being released as a public Go module that provides a Go API for devices to interact with the MODE cloud.


## Installation

Our recommendation is to use the package as a module. In module mode, Go will
automatically download the module when it sees an `import` statement
referencing this repository. If it is not already, your workspace should be a
Go module which will be able to consume this one:

```
go mod init <your module path>
```

This will allow go you to download and install the module at build or run time.

If you are not yet using Go modules and are still in `GOPATH` mode, you may use
`go get` to fetch the SDK:

```
go get github.com/moderepo/device-sdk-go/v4
```

## Using the SDK

You will be using the `mode` package. For example:
```
package main

import (
    "fmt"
    "github.com/moderepo/device-sdk-go/v4/mode"
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

See more examples [here](https://github.com/moderepo/device-sdk-go/blob/master/v4/examples).


## Documentation

See the full API documentation [here](https://pkg.go.dev/github.com/moderepo/device-sdk-go/v4).


## Copyright and License

Code and documentation copyright 2024 Mode, Inc. Released under the [MIT license](https://github.com/moderepo/device-sdk-go/blob/master/v4/LICENSE).
