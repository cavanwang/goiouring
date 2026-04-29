//go:build linux

package common

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

// UringManager 管理 io_uring 实例的生命周期、资源池及结果分发。
type UringManager struct {
	ring      *Ring
	bufPool   *MultiPool
	pending   sync.Map
	requestID uint64
	sqeWaiter chan struct{}
	mu        sync.Mutex
}

type requestCtx struct {
	resCh    chan Result
	bufPtr   *[]byte
	msg      *unix.Msghdr
	sockaddr *unix.RawSockaddrAny
}

func NewUringManager(r *Ring, p *MultiPool) *UringManager {
	m := &UringManager{
		ring:      r,
		bufPool:   p,
		sqeWaiter: make(chan struct{}, 1),
	}
	go m.reaper()
	return m
}

func (m *UringManager) reaper() {
	for {
		userData, res, err := m.ring.WaitCQE()
		if err != nil {
			return
		}

		select {
		case m.sqeWaiter <- struct{}{}:
		default:
		}

		if userData == 0 {
			continue
		}

		if ctxInterface, ok := m.pending.LoadAndDelete(userData); ok {
			ctx := ctxInterface.(*requestCtx)
			if ctx.bufPtr != nil {
				m.bufPool.Put(ctx.bufPtr)
			}

			if ctx.resCh != nil {
				var resErr error
				if res < 0 {
					errno := unix.Errno(-res)
					if errno == unix.ETIME || errno == unix.ECANCELED {
						resErr = os.ErrDeadlineExceeded
					} else {
						resErr = errno
					}
					res = 0
				}
				select {
				case ctx.resCh <- Result{N: int(res), Err: resErr}:
				default:
				}
			}
		}
	}
}

type UringConn struct {
	// 【核心修复点】预分配固定内存，防止异步期间 timespec 被移动
	readTs        *kernelTimespec
	writeTs       *kernelTimespec
	fd            int
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
	defaultManager = NewUringManager(r, NewMultiPool())
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
	fd := int(f.Fd())

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
	if err != nil {
		return nil, err
	}
	if err := rawConn.Close(); err != nil {
		return nil, err
	}
	fd := int(f.Fd())

	// 【修复点】不再调用 rawConn.Close()。
	// tc.File() 已经分离了 FD 状态，此时 Close 原连接会导致底层 socket 状态异常触发 EBADF。
	// 原 rawConn 将随对象生命周期结束被 GC。

	c := &UringConn{
		fd:      fd,
		manager: manager,
		file:    f,
		laddr:   laddr,
		raddr:   raddr,
		readTs:  new(kernelTimespec),
		writeTs: new(kernelTimespec),
	}
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
	msghdr := &unix.Msghdr{
		Name:    (*byte)(unsafe.Pointer(sa)),
		Namelen: uint32(saLen),
		Iov:     &iov,
		Iovlen:  1,
	}

	u.manager.pending.Store(id, &requestCtx{
		bufPtr: bufPtr,
		msg:    msghdr,
	})

	numSQEs := uint32(1)
	if hasTimeout {
		numSQEs = 2
	}

	u.manager.mu.Lock()
	sqes := u.getSQEsBlocking(numSQEs)

	sqe := sqes[0]
	sqe.Opcode = IORING_OP_SENDMSG
	sqe.Fd = int32(u.fd)
	sqe.Addr = uint64(uintptr(unsafe.Pointer(msghdr)))
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

	sa := new(unix.RawSockaddrAny)
	iov := unix.Iovec{Base: &b[0], Len: uint64(len(b))}
	msghdr := &unix.Msghdr{
		Name:    (*byte)(unsafe.Pointer(sa)),
		Namelen: uint32(unix.SizeofSockaddrAny),
		Iov:     &iov,
		Iovlen:  1,
	}

	u.manager.pending.Store(id, &requestCtx{
		resCh:    ch,
		msg:      msghdr,
		sockaddr: sa,
	})

	numSQEs := uint32(1)
	if hasTimeout {
		numSQEs = 2
	}

	u.manager.mu.Lock()
	sqes := u.getSQEsBlocking(numSQEs)

	sqe := sqes[0]
	sqe.Opcode = IORING_OP_RECVMSG
	sqe.Fd = int32(u.fd)
	sqe.Addr = uint64(uintptr(unsafe.Pointer(msghdr)))
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

	netAddr, _ := parseSockaddr(sa)
	runtime.KeepAlive(u)
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
