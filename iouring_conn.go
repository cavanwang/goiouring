//go:build linux

package goiouring

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	log "log/slog"
)

var (
	// 验证 UringConn 是否完全符合 net.Conn 接口标准
	_ net.Conn = (*UringConn)(nil)

	// readResultChanPool 用于复用 Read 操作的结果通知通道。
	readResultChanPool = sync.Pool{
		New: func() any {
			return make(chan Result, 1)
		},
	}

	defaultManager *UringManager
)

const (
	IOSQE_FIXED_FILE  = 1 << 0 // 1
	IOSQE_IO_DRAIN    = 1 << 1 // 2
	IOSQE_IO_LINK     = 1 << 2 // 4  <-- 正确值应该是 4
	IOSQE_IO_HARDLINK = 1 << 3 // 8
)

// kernelTimespec 对应 Linux 内核定义的 __kernel_timespec。
type kernelTimespec struct {
	tv_sec  int64
	tv_nsec int64
}

// Result 存储异步 IO 操作的最终返回状态。
type Result struct {
	N   int
	Err error
}

type requestCtx struct {
	resCh  chan Result
	bufPtr *[]byte

	// 关键：直接存储结构体，而不是指针
	// 这样这些内存会随着 requestCtx 一起分配在堆上
	msg unix.Msghdr
	iov unix.Iovec
	sa  unix.RawSockaddrAny
}

type UringConn struct {
	// 【核心修复点】预分配固定内存，防止异步期间 timespec 被移动
	readTs        *kernelTimespec
	writeTs       *kernelTimespec
	fd            int32
	manager       *UringManager
	laddr         net.Addr
	raddr         net.Addr
	file          *os.File
	readDeadline  atomic.Pointer[time.Time]
	writeDeadline atomic.Pointer[time.Time]
}

func InitRing() {
	fmt.Println("will calling NewRing")
	r, err := NewRing(4096)
	if err != nil {
		fmt.Println("failed to create io_uring ring:", err.Error())
		panic(err)
	}
	fmt.Println("will calling NewUringManager")
	defaultManager = NewUringManager(r, NewMultiPool(), time.Millisecond, 100)
	fmt.Println("done: calling NewUringManager")
}

func NewDefaultUringConn(rawConn net.Conn) (net.Conn, error) {
	return NewUringConn(rawConn, defaultManager)
}

func NewDefaultUringUDPConn(rawConn net.PacketConn) (net.PacketConn, error) {
	return NewUringUDPConn(rawConn, defaultManager)
}

func NewUringUDPConn(rawConn net.PacketConn, manager *UringManager) (net.PacketConn, error) {
	laddr := rawConn.LocalAddr()

	tc, ok := rawConn.(interface{ File() (*os.File, error) })
	if !ok {
		return nil, fmt.Errorf("not a file-based connection")
	}

	f, err := tc.File() // 执行了 dup()
	if err != nil {
		return nil, err
	}
	if err := rawConn.Close(); err != nil {
		return nil, err
	}
	fd := int32(f.Fd())

	// 【修复点】不再调用 rawConn.Close()。
	// tc.File() 已经分离了 FD 状态，此时 Close 原连接会导致底层 socket 状态异常触发 EBADF。
	// 原 rawConn 将随对象生命周期结束被 GC。

	c := &UringConn{
		fd:      fd,
		manager: manager,
		file:    f,
		laddr:   laddr,
		readTs:  new(kernelTimespec),
		writeTs: new(kernelTimespec),
	}
	runtime.KeepAlive(c.readTs)
	runtime.KeepAlive(c.writeTs)
	return c, nil
}

func NewUringConn(rawConn net.Conn, manager *UringManager) (net.Conn, error) {
	laddr := rawConn.LocalAddr()
	raddr := rawConn.RemoteAddr()

	tc, ok := rawConn.(interface{ File() (*os.File, error) })
	if !ok {
		return nil, fmt.Errorf("not a file-based connection")
	}

	f, err := tc.File() // 执行了 dup()
	log.Info("tc.File() called")
	if err != nil {
		return nil, err
	}
	if err := rawConn.Close(); err != nil {
		return nil, err
	}
	fd := int(f.Fd())
	log.Info("rawconn.close called")

	// 【修复点】不再调用 rawConn.Close()。
	// tc.File() 已经分离了 FD 状态，此时 Close 原连接会导致底层 socket 状态异常触发 EBADF。
	// 原 rawConn 将随对象生命周期结束被 GC。

	c := &UringConn{
		fd:      int32(fd),
		manager: manager,
		file:    f,
		laddr:   laddr,
		raddr:   raddr,
		readTs:  new(kernelTimespec),
		writeTs: new(kernelTimespec),
	}
	c.Write(nil)
	runtime.KeepAlive(c.readTs)
	runtime.KeepAlive(c.writeTs)
	return c, nil
}

func (u *UringConn) LocalAddr() net.Addr  { return u.laddr }
func (u *UringConn) RemoteAddr() net.Addr { return u.raddr }

func (u *UringConn) SetDeadline(t time.Time) error {
	u.SetReadDeadline(t)
	u.SetWriteDeadline(t)
	return nil
}

func (u *UringConn) SetReadDeadline(t time.Time) error {
	u.readDeadline.Store(&t)
	return nil
}

func (u *UringConn) SetWriteDeadline(t time.Time) error {
	u.writeDeadline.Store(&t)
	return nil
}

func (u *UringConn) File() (*os.File, error) {
	return u.file, nil
}

func (u *UringConn) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	hasTimeout := u.fillTs(u.writeTs, u.writeDeadline.Load())

	bufPtr := u.manager.bufPool.Get(len(b))
	//log.Debug("Write: %v bufPtr=%p bufPtr.len =%d idx=%d len(b)=%d", u.RemoteAddr(), bufPtr, len(*bufPtr), poolIdx, len(b))
	copy((*bufPtr)[:len(b)], b)
	*bufPtr = (*bufPtr)[:len(b)]

	id := atomic.AddUint64(&u.manager.requestID, 1)
	u.manager.pending.Store(id, &requestCtx{
		bufPtr: bufPtr,
	})

	numSQEs := uint32(1)
	if hasTimeout {
		numSQEs = 2
	}

	u.manager.mu.Lock()
	sqes := u.getSQEsBlocking(numSQEs)

	sqe := sqes[0]
	sqe.Opcode = IORING_OP_WRITE
	sqe.Fd = int32(u.fd)
	//log.Debug("now Write for conn %v: bufPtr=%p bufPtr.len = %d len(b)=%d", u.RemoteAddr(), bufPtr, len(*bufPtr), len(b))
	sqe.Addr = uint64(uintptr(unsafe.Pointer(&(*bufPtr)[0])))
	sqe.Len = uint32(len(b))
	sqe.UserData = id

	if hasTimeout {
		sqe.Flags |= IOSQE_IO_LINK
		tsqe := sqes[1]
		tsqe.Opcode = IORING_OP_LINK_TIMEOUT
		tsqe.Fd = -1
		tsqe.Addr = uint64(uintptr(unsafe.Pointer(u.writeTs)))
		tsqe.Len = 1
		tsqe.UserData = 0
	}

	u.manager.ring.FlushSQEs(numSQEs)
	if err := u.manager.ring.Submit(numSQEs); err != nil {
		u.manager.mu.Unlock()
		u.manager.pending.Delete(id)
		//log.Debug("Write failed for conn %v put buf=%p, buflen=%d idx=%d: %v", u.RemoteAddr(), bufPtr, len(*bufPtr), poolIdx, err)
		u.manager.bufPool.Put(bufPtr)
		return 0, err
	}
	u.manager.mu.Unlock()

	//log.Debug("Written for conn %v: bufPtr=%p bufPtr.len = %d idx=%d len(b)=%d", u.RemoteAddr(), bufPtr, len(*bufPtr), poolIdx, len(b))
	return len(b), nil
}

func (u *UringConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	hasTimeout := u.fillTs(u.writeTs, u.writeDeadline.Load())
	bufPtr := u.manager.bufPool.Get(len(b))
	copy((*bufPtr)[:len(b)], b)

	sa, saLen, err := addrToSockaddr(addr)
	if err != nil {
		return 0, err
	}

	id := atomic.AddUint64(&u.manager.requestID, 1)
	iov := unix.Iovec{Base: &(*bufPtr)[0], Len: uint64(len(b))}
	reqCtx := &requestCtx{
		bufPtr: bufPtr,
		msg: unix.Msghdr{
			Name:    (*byte)(unsafe.Pointer(sa)),
			Namelen: uint32(saLen),
			Iov:     &iov,
			Iovlen:  1,
		},
	}

	u.manager.pending.Store(id, reqCtx)

	numSQEs := uint32(1)
	if hasTimeout {
		numSQEs = 2
	}

	u.manager.mu.Lock()
	sqes := u.getSQEsBlocking(numSQEs)

	sqe := sqes[0]
	sqe.Opcode = IORING_OP_SENDMSG
	sqe.Fd = int32(u.fd)
	sqe.Addr = uint64(uintptr(unsafe.Pointer(&reqCtx.msg)))
	sqe.Len = 1
	sqe.UserData = id

	if hasTimeout {
		sqe.Flags |= IOSQE_IO_LINK
		tsqe := sqes[1]
		tsqe.Opcode = IORING_OP_LINK_TIMEOUT
		tsqe.Fd = -1
		tsqe.Addr = uint64(uintptr(unsafe.Pointer(u.writeTs)))
		tsqe.Len = 1
		tsqe.UserData = 0
	}

	u.manager.ring.FlushSQEs(numSQEs)
	if err := u.manager.ring.Submit(numSQEs); err != nil {
		u.manager.mu.Unlock()
		u.manager.pending.Delete(id)
		u.manager.bufPool.Put(bufPtr)
		return 0, err
	}
	u.manager.mu.Unlock()

	return len(b), nil
}

func (u *UringConn) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	// 强制让 b 逃逸到堆
	runtime.KeepAlive(&b[0])

	// 1. 获取调用栈，查明是谁在调用 Read
	//st := make([]byte, 2048)
	//nn := runtime.Stack(st, false)
	//log.Debug("[Read][Trace] Call Stack:\n%s", st[:nn])

	// 2. 核心状态检查：验证 FD 是否已被意外关闭
	// F_GETFL 如果返回错误，说明 FD 已经失效（Bad File Descriptor）
	//fl, fcntlErr := unix.FcntlInt(uintptr(u.fd), unix.F_GETFL, 0)
	//if fcntlErr != nil {
	//	log.Debug("[Read][Critical] FD %d is INVALID before SQE prep: %v", u.fd, fcntlErr)
	//} else {
	//	log.Debug("[Read][Status] FD %d is VALID, Flags: %d, Remote: %v", u.fd, fl, u.RemoteAddr())
	//}

	// 3. 准备超时和 ID
	hasTimeout := u.fillTs(u.readTs, u.readDeadline.Load())
	id := atomic.AddUint64(&u.manager.requestID, 1)
	ch := readResultChanPool.Get().(chan Result)
	u.manager.pending.Store(id, &requestCtx{resCh: ch})

	numSQEs := uint32(1)
	if hasTimeout {
		numSQEs = 2
	}

	u.manager.mu.Lock()
	sqes := u.getSQEsBlocking(numSQEs)

	//log.Debug("DEBUG: SQE0 Addr: %p, SQE1 Addr: %p, Diff: %d", sqes[0], sqes[1], uintptr(unsafe.Pointer(sqes[1]))-uintptr(unsafe.Pointer(sqes[0])))

	// 4. 填充 READ SQE
	sqe := sqes[0]
	*sqe = SQE{} // 彻底清空槽位
	sqe.Opcode = IORING_OP_READ
	sqe.Fd = int32(u.fd)
	sqe.Addr = uint64(uintptr(unsafe.Pointer(&b[0])))
	sqe.Len = uint32(len(b))
	sqe.UserData = id
	if hasTimeout {
		sqe.Flags |= IOSQE_IO_LINK
	}

	//log.Debug("[Read][SQE0] Op: READ, FD: %d, Addr: 0x%x, Len: %d, Flags: %d, UserData: %d",
	//	sqe.Fd, sqe.Addr, sqe.Len, sqe.Flags, sqe.UserData)

	// 5. 填充 LINK_TIMEOUT SQE
	if hasTimeout {
		tsqe := sqes[1]
		*tsqe = SQE{}
		tsqe.Opcode = IORING_OP_LINK_TIMEOUT
		//tsqe.Opcode = 0
		tsqe.Fd = -1 // 必须是 -1
		tsqe.Addr = uint64(uintptr(unsafe.Pointer(u.readTs)))
		tsqe.Len = 1
		tsqe.UserData = 0

		//log.Debug("[Read][SQE1] tsqe Op: %d, FD: %d, Addr: 0x%x (Aligned8: %v), Off: %d",
		//	tsqe.Opcode, tsqe.Fd, tsqe.Addr, tsqe.Addr%8 == 0, tsqe.Off)
		//
		//log.Debug("[Read][SQE1] Op: LINK_TIMEOUT, FD: %d, Addr(Ts): 0x%x, Len: %d, Val: %+v",
		//	tsqe.Fd, tsqe.Addr, tsqe.Len, u.readTs)
	}

	// 6. 提交到环
	u.manager.ring.FlushSQEs(numSQEs)
	submitErr := u.manager.ring.Submit(numSQEs)
	u.manager.mu.Unlock()

	if submitErr != nil {
		u.manager.pending.Delete(id)
		//log.Debug("[Read][Error] io_uring submit failed: %v", submitErr)
		return 0, submitErr
	}
	//log.Debug("[Read][Submit] ID %d submitted successfully, waiting for CQE...", id)

	// 7. 等待结果
	res := <-ch
	readResultChanPool.Put(ch)

	// 8. 结果诊断日志
	if res.Err != nil {
		log.Debug("[Read][Result] FAILED: ID %d, N: %d, Err: %v", id, res.N, res.Err)
		// 如果还是 EBADF，查看这一瞬间 FD 是否还活着
		//_, fcntlErr2 := unix.FcntlInt(uintptr(u.fd), unix.F_GETFL, 0)
		//log.Debug("[Read][Post-Mortem] FD %d alive check: %v", u.fd, fcntlErr2)
	} else {
		//log.Debug("[Read][Result] SUCCESS: ID %d, N: %d", id, res.N)
	}

	// 强制保持连接对象不被回收
	runtime.KeepAlive(u)

	if res.Err == nil && res.N == 0 {
		res.Err = io.EOF
	}
	return res.N, res.Err
}

func (u *UringConn) SetReadBuffer(bytes int) error {
	return unix.SetsockoptInt(int(u.fd), unix.SOL_SOCKET, unix.SO_RCVBUF, bytes)
}

// SetWriteBuffer 设置内核套接字发送缓冲区大小
func (u *UringConn) SetWriteBuffer(bytes int) error {
	// 强制限制：Linux 内核会对这个值进行翻倍，以预留出 sk_buff 结构开销
	return unix.SetsockoptInt(int(u.fd), unix.SOL_SOCKET, unix.SO_SNDBUF, bytes)
}

func (u *UringConn) GetWriteBuffer() (int, error) {
	return unix.GetsockoptInt(int(u.fd), unix.SOL_SOCKET, unix.SO_SNDBUF)
}

func (u *UringConn) GetReadBuffer() (int, error) {
	return unix.GetsockoptInt(int(u.fd), unix.SOL_SOCKET, unix.SO_RCVBUF)
}

func (u *UringConn) Close() error {
	//b := make([]byte, 4096)
	//nn := runtime.Stack(b, false)
	log.Debug("will calling Close for conn v", u.RemoteAddr())
	return u.file.Close()
}

func (u *UringConn) getSQEsBlocking(n uint32) []*SQE {
	spin := 0
	for {
		sqes := u.manager.ring.GetSQEs(n)
		if sqes != nil {
			for i := range sqes {
				*sqes[i] = SQE{}
			}
			return sqes
		}
		if spin < 10 {
			spin++
			runtime.Gosched()
			continue
		}
		u.manager.ring.Submit(uint32(len(sqes)))
		<-u.manager.sqeWaiter
	}
}

func parseSockaddr(sa *unix.RawSockaddrAny) (net.Addr, error) {
	switch sa.Addr.Family {
	case unix.AF_INET:
		pp := (*unix.RawSockaddrInet4)(unsafe.Pointer(sa))
		port := binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&pp.Port))[:])
		return &net.UDPAddr{IP: pp.Addr[:], Port: int(port)}, nil
	case unix.AF_INET6:
		pp := (*unix.RawSockaddrInet6)(unsafe.Pointer(sa))
		port := binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&pp.Port))[:])
		return &net.UDPAddr{IP: pp.Addr[:], Port: int(port)}, nil
	}
	return nil, fmt.Errorf("unsupported address family")
}

func addrToSockaddr(addr net.Addr) (*unix.RawSockaddrAny, uint32, error) {
	udp, ok := addr.(*net.UDPAddr)
	if !ok {
		return nil, 0, fmt.Errorf("only UDP supported")
	}

	if ip4 := udp.IP.To4(); ip4 != nil {
		sa := new(unix.RawSockaddrInet4)
		sa.Family = unix.AF_INET
		binary.BigEndian.PutUint16((*[2]byte)(unsafe.Pointer(&sa.Port))[:], uint16(udp.Port))
		copy(sa.Addr[:], ip4)
		return (*unix.RawSockaddrAny)(unsafe.Pointer(sa)), unix.SizeofSockaddrInet4, nil
	}

	sa := new(unix.RawSockaddrInet6)
	sa.Family = unix.AF_INET6
	binary.BigEndian.PutUint16((*[2]byte)(unsafe.Pointer(&sa.Port))[:], uint16(udp.Port))
	copy(sa.Addr[:], udp.IP.To16())
	return (*unix.RawSockaddrAny)(unsafe.Pointer(sa)), unix.SizeofSockaddrInet6, nil
}

// wrapProtocolError 统一封装协议不支持的错误
func (u *UringConn) wrapProtocolError(op string) error {
	network := "unknown"
	if u.laddr != nil {
		network = u.laddr.Network()
	}

	return u.wrapError(op, network, unix.ENOPROTOOPT)
}

// isUDP 辅助判断，让逻辑更清晰
func (u *UringConn) isUDP() bool {
	_, ok := u.laddr.(*net.UDPAddr)
	return ok
}

func (u *UringConn) wrapError(op, netType string, err error) error {
	return &net.OpError{
		Op:     op,
		Net:    netType,
		Source: u.laddr,
		Addr:   u.raddr,
		Err:    err,
	}
}

// 【修复点】修改为填充固定成员地址
func (u *UringConn) fillTs(ts *kernelTimespec, t *time.Time) bool {
	if t == nil || t.IsZero() {
		return false
	}
	d := time.Until(*t)
	if d <= 0 {
		ts.tv_sec = 0
		ts.tv_nsec = 1
		return true
	}
	ts.tv_sec = int64(d.Seconds())
	ts.tv_nsec = int64(d.Nanoseconds() % 1e9)
	return true
}
