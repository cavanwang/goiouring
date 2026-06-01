package main

import (
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	_ "net/http/pprof"

	"github.com/cavanwang/goiouring"
)

func main() {
	// 开启一个独立的协程，专门监听 pprof 端口（比如 6060）
	go func() {
		log.Println("Pprof server is running on :26060")
		// 访问 http://localhost:6060/debug/pprof/
		if err := http.ListenAndServe(":26060", nil); err != nil {
			log.Fatalf("pprof failed: %v", err)
		}
	}()

	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer l.Close()

	ring, err := goiouring.NewRing(4)
	if err != nil {
		panic(err)
	}
	pool := goiouring.NewMultiPool()
	mgr := goiouring.NewUringManager(ring, pool, time.Second*5, 64)

	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		conn, err = goiouring.NewUringConn(conn, mgr)
		if err != nil {
			panic(err)
		}
		go func(conn net.Conn) {
			defer conn.Close()
			b := make([]byte, 1024)
			i := 0
			for {
				n, err := conn.Read(b)
				if err != nil {
					if errors.Is(err, io.EOF) {
						return
					}
					panic(err)
				}
				i++
				if i%1000 == 0 {
					log.Printf("conn=%p i=%d read %d bytes: %s\n", conn, i, n, b[:n])
				}
				n, err = conn.Write(b[:n])
				if err != nil {
					panic(err)
				}
				if i%1000 == 0 {
					log.Printf("conn=%p i=%d write %d bytes: %s\n", conn, i, n, b[:n])
				}
			}
		}(conn)
	}
}
