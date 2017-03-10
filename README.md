# MODE Device SDK for Go

This SDK is for anyone implementing MODE device drivers in the Go language. It is
being released as a public Go package that provides a Go API for devices to interact with the MODE cloud.


## Installation

You can install this package simply by doing:

    $ go get github.com/moderepo/device-sdk-go

However, we strongly recommend you use [Glide](https://glide.sh/) to manage your
package dependencies. This package comes with its own `glide.yaml` file so that
the secondary dependencies are taken care of. To use Glide to install this package,
simply do:

    $ glide get github.com/moderepo/device-sdk-go


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
