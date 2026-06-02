package main

import (
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"time"
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

	/*mgr, err := goiouring.NewUringManager(1024, 1024*50)
	if err != nil {
		panic(err)
	}*/
	var t atomic.Int64
	var lastPairs int64
	go func() {
		for {
			time.Sleep(time.Second * 2)
			log.Printf("total read/write pairs = %d/s", (t.Load()-lastPairs)/2)
			lastPairs = t.Load()
		}
	}()

	var lastT atomic.Int64
	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		/*conn, err = goiouring.NewUringConn(conn, mgr)
		if err != nil {
			panic(err)
		}*/
		go func(conn net.Conn) {
			defer conn.Close()
			b := make([]byte, 1024)
			i := 0
			for {
				n, err := conn.Read(b)
				if err != nil {
					if errors.Is(err, io.EOF) {
						log.Printf("read %d bytes EOF", n)
						return
					}
					panic(err)
				}
				i++
				n, err = conn.Write(b[:n])
				if err != nil {
					panic(err)
				}
				t.Add(1)
				now := time.Now().Unix()
				old := lastT.Load()
				if now-old > 5 {
					if lastT.CompareAndSwap(old, now) {
						log.Printf("conn=%p i=%d write %d bytes: %s\n", conn, i, n, b[:min(n, 10)])
					}
				}
			}
		}(conn)
	}
}
