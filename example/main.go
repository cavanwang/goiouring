package main

import (
	"net"
)

func main() {
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer l.Close()

	goiouring.X

	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}

	}
}
