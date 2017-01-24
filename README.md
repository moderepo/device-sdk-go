# MODE Device SDK for Go

This SDK is for anyone implement MODE device drivers using the Go language. It is
being released as a public Go package that provides a Go API for devices to interact with the MODE cloud.

## Installation

You can install this package simply by doing:

    $ go get github.com/moderepo/device-sdk-go

However, we strongly recommend you use [Glide](https://glide.sh/) to manage your
package dependencies. This package comes with its own `glide.yaml` file so that
the secondary dependencies are taken care of. To use Glide to install this package,
simply do:

    $ glide get github.com/moderepo/device-sdk-go


## Copyright and License

Code and documentation copyright 2017 Mode, Inc. Released under the [MIT license](https://github.com/moderepo/device-sdk-go/blob/master/LICENSE).
