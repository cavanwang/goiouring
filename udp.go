package goiouring

import (
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

func (u *UringConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	if len(b) == 0 {
		return 0, nil, nil
	}
	// 强制让 b 逃逸到堆
	runtime.KeepAlive(&b[0])

	//bb := make([]byte, 4096)
	//nn := runtime.Stack(bb, false)
	//fmt.Printf("ReadFrom: will calling Read for conn %v: from:\n%s", u.RemoteAddr(), bb[:nn])

	hasTimeout := u.fillTs(u.readTs, u.readDeadline.Load())
	id := atomic.AddUint64(&u.manager.requestID, 1)
	ch := readResultChanPool.Get().(chan Result)

	// 初始化 ctx
	ctx := &requestCtx{
		resCh: ch,
	}
	// 设置 iov
	ctx.iov.Base = &b[0]
	ctx.iov.Len = uint64(len(b))
	// 设置 msghdr
	ctx.msg.Name = (*byte)(unsafe.Pointer(&ctx.sa)) // 指向 ctx 内部的 sa
	ctx.msg.Namelen = uint32(unix.SizeofSockaddrAny)
	ctx.msg.Iov = &ctx.iov // 指向 ctx 内部的 iov
	ctx.msg.Iovlen = 1
	// 存储 ctx
	u.manager.pending.Store(id, ctx)

	numSQEs := uint32(1)
	if hasTimeout {
		numSQEs = 2
	}

	u.manager.mu.Lock()
	sqes := u.getSQEsBlocking(numSQEs)

	sqe := sqes[0]
	sqe.Opcode = IORING_OP_RECVMSG
	sqe.Fd = int32(u.fd)
	// 核心：这里的 Addr 必须指向 ctx 里的 msg，且 ctx 此时在 Map 里，是安全的
	sqe.Addr = uint64(uintptr(unsafe.Pointer(&ctx.msg)))
	sqe.Len = 1
	sqe.UserData = id

	if hasTimeout {
		sqe.Flags |= IOSQE_IO_LINK
		tsqe := sqes[1]
		tsqe.Opcode = IORING_OP_LINK_TIMEOUT
		tsqe.Fd = -1
		tsqe.Addr = uint64(uintptr(unsafe.Pointer(u.readTs)))
		tsqe.Len = 1
		tsqe.UserData = 0
	}

	u.manager.ring.FlushSQEs(numSQEs)
	if err := u.manager.ring.Submit(numSQEs); err != nil {
		u.manager.mu.Unlock()
		u.manager.pending.Delete(id)
		readResultChanPool.Put(ch)
		return 0, nil, err
	}
	u.manager.mu.Unlock()

	res := <-ch
	readResultChanPool.Put(ch)
	//fmt.Printf("ReadFrom: got %d bytes for conn %v, err=%v\n", res.N, u.RemoteAddr(), res.Err)

	if res.Err != nil {
		return 0, nil, res.Err
	}
	if res.N == 0 {
		return 0, nil, io.EOF
	}

	netAddr, _ := parseSockaddr(&ctx.sa)
	return res.N, netAddr, nil
}

func (u *UringConn) ReadFromUDP(b []byte) (n int, addr *net.UDPAddr, err error) {
	n, a, err := u.ReadFrom(b)
	if a != nil {
		return n, a.(*net.UDPAddr), err
	}
	return n, nil, err
}

func (u *UringConn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	return u.WriteTo(b, addr)
}
