# go iouring 

A high-performance, asynchronous networking library for Go, powered by `io_uring`. 

This project provides a seamless wrapper for `net.Conn` and `net.PacketConn`, enabling Go applications to leverage Linux Proactor IO without rewriting business logic.

## 🚀 Key Features

- **Proactor Model**: Truly asynchronous IO operations.
- **Protocol Support**: Full support for both TCP and UDP.
- **Drop-in Replacement**: Compatible with `net.Conn` and `net.PacketConn` interfaces.
- **Memory Efficient**: Integrated buffer pooling to minimize GC pressure.
- **Deadline Support**: Native support for `SetDeadline` using `IORING_OP_LINK_TIMEOUT`.

## 🛠 Usage

```go
// Initialize the ring
common.InitRing()

// Wrap a standard UDP connection
rawConn, _ := net.ListenUDP("udp", addr)
uringConn, _ := common.NewDefaultUringUDPConn(rawConn)

// Use it as a normal PacketConn
buf := make([]byte, 1024)
n, addr, err := uringConn.ReadFrom(buf)
