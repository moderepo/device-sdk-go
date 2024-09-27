Package `packet` implements functionality for encoding and decoding [MQTT 3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/) packets.

This package originates from [gomqtt](https://github.com/gomqtt), which in turn originates from [surgemq](https://github.com/surgemq/surgemq).
However, both repos are no longer public.

This gomqtt authors are:
- Jian Zhen <zhenjl@gmail.com>
- Joël Gähwiler <joel.gaehwiler@gmail.com>


## Usage

```go
/* Packet Encoding */

// Create new packet.
pkt1 := NewConnectPacket()
pkt1.Username = "gomqtt"
pkt1.Password = "amazing!"

// Allocate buffer.
buf := make([]byte, pkt1.Len())

// Encode the packet.
if _, err := pkt1.Encode(buf); err != nil {
    panic(err) // error while encoding
}

/* Packet Decoding */

// Detect packet.
l, mt := DetectPacket(buf)

// Check length
if l == 0 {
    return // buffer not complete yet
}

// Create packet.
pkt2, err := mt.New();
if err != nil {
    panic(err) // packet type is invalid
}

// Decode packet.
_, err = pkt2.Decode(buf)
if err != nil {
    panic(err) // there was an error while decoding
}

switch pkt2.Type() {
case CONNECT:
    c := pkt2.(*ConnectPacket)
    fmt.Println(c.Username)
    fmt.Println(c.Password)
}

// Output:
// gomqtt
// amazing!
```

