//go:build linux

package goiouring

import (
	"encoding/binary"
	"errors"
	"fmt"
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

	ioTaskPool = sync.Pool{
		New: func() interface{} {
			return &IOTask{
				// 每个 Task 自带一个容量为 1 的无锁通知管道
				ResChan: make(chan int, 1),
			}
		},
	}

	// 全局的 IOTask 对象池 (针对 UDP 场景定制)
	udpWriteTaskPool = sync.Pool{
		New: func() interface{} {
			return &IOTask{
				ResChan: make(chan int, 1),
				// 预分配足够的空间存放 sockaddr，避免每次都分配内存
				SockAddr: make([]byte, 128),
			}
		},
	}

	ErrUnsupportedAddr = errors.New("unsupported address type, must be UDPAddr")
	ErrBufferTooSmall  = errors.New("provided sockaddr buffer is too small")
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
	laddr         net.Addr
	raddr         net.Addr
	file          *os.File
	readDeadline  atomic.Pointer[time.Time]
	writeDeadline atomic.Pointer[time.Time]

	r *Ring
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

	c := &UringConn{
		fd:      fd,
		r:       manager.GetRingForFD(int32(fd)),
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

	c := &UringConn{
		fd:      int32(fd),
		file:    f,
		laddr:   laddr,
		raddr:   raddr,
		readTs:  new(kernelTimespec),
		writeTs: new(kernelTimespec),
		r:       manager.GetRingForFD(int32(fd)),
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

func (u *UringConn) File() (*os.File, error) {
	return u.file, nil
}

var (
	readTimes  atomic.Int64
	writeTimes atomic.Int64
)

func (u *UringConn) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	task := ioTaskPool.Get().(*IOTask)

	task.OpCode = IORING_OP_SEND
	task.Fd = u.fd
	task.Buf = b
	task.Offset = 0
	task.Err = nil

	ts, hasTimeout, tErr := convertDeadline(&u.writeDeadline)
	if tErr != nil {
		// 已超时错误直接拦截返回
		udpWriteTaskPool.Put(task)
		return 0, tErr
	}
	if hasTimeout {
		task.HasTimeout = 1
		task.Timespec = ts
	}

	u.r.PushTask(task)

	// 等待内核把 TCP 缓冲区的数据发出去
	n = <-task.ResChan
	err = task.Err

	runtime.KeepAlive(b)
	runtime.KeepAlive(task)

	ioTaskPool.Put(task)

	return n, err
}

func (u *UringConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	// 1. 从对象池捞取 Task
	task := udpWriteTaskPool.Get().(*IOTask)
	task.Err = nil
	task.HasTimeout = 0

	task.Buf = b

	// 2. 解析并持久化套接字地址
	saLen, err := addrToSockaddr(addr, task.SockAddr)
	if err != nil {
		udpWriteTaskPool.Put(task)
		return 0, err
	}

	// 3. 处理硬件超时
	ts, hasTimeout, tErr := convertDeadline(&u.writeDeadline)
	if tErr != nil {
		udpWriteTaskPool.Put(task)
		return 0, tErr
	}
	if hasTimeout {
		task.HasTimeout = 1
		task.Timespec = ts
	}

	// 4. 组装内存完美的 Msghdr
	task.Iov.Base = &task.Buf[0]
	task.Iov.Len = uint64(len(b))

	task.Msg.Name = (*byte)(unsafe.Pointer(&task.SockAddr[0]))
	task.Msg.Namelen = uint32(saLen)
	task.Msg.Iov = &task.Iov
	task.Msg.Iovlen = 1
	task.Msg.Control = nil
	task.Msg.Controllen = 0
	task.Msg.Flags = 0

	// 5. 基础字段初始化
	task.OpCode = IORING_OP_SENDMSG
	task.Fd = int32(u.fd)

	// 6. 【终极无锁推包】甩给后台发动机
	u.r.PushTask(task)

	// 7. 挂起等待内核 DMA 发送完毕信号
	n := <-task.ResChan
	resErr := task.Err

	// 8. 保证安全防线（额外对 b 施加保护，防止极端优化下编译器提前回收切片底座）
	runtime.KeepAlive(b)
	runtime.KeepAlive(task)

	// 9. 资源安全回收
	udpWriteTaskPool.Put(task)

	if resErr != nil {
		return 0, resErr
	}
	return n, nil
}

func (u *UringConn) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	// 1. 从对象池捞一个干净的 IOTask
	task := ioTaskPool.Get().(*IOTask)

	// 2. 严格初始化该 Task 的内核所需字段
	task.OpCode = IORING_OP_READ
	task.Fd = u.fd
	task.Buf = b
	task.Offset = 0 // 网络 I/O 偏移量一律填 0
	task.Err = nil  // 必须重置错误，防止上一次复用的残留

	ts, hasTimeout, tErr := convertDeadline(&u.readDeadline)
	if tErr != nil {
		udpReadTaskPool.Put(task)
		return 0, tErr
	}
	if hasTimeout {
		task.HasTimeout = 1
		task.Timespec = ts
	}

	// 3. 将任务推入 startPollToDoTasks 的待办管道
	u.r.PushTask(task)

	// 4. 【致命阻塞点】当前业务协程在此挂起，把 CPU 让给别人。
	// 当 startPollDoneTasks 收割到内核的 CQE 后，会通过这个 Channel 唤醒我们
	n = <-task.ResChan
	err = task.Err

	// 5. 【护城河】确保在拿到结果前，buf 和 task 绝对不被 GC 动弹或做栈重排
	runtime.KeepAlive(b)
	runtime.KeepAlive(task)

	// 6. 擦干净，还给对象池
	ioTaskPool.Put(task)

	return n, err
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
	log.Debug("will calling Close for conn v", u.RemoteAddr())
	return u.file.Close()
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

// addrToSockaddr 将 Go 的 net.Addr 零分配地写入传入的 saBuf 中
// 返回值 saLen 代表实际塞入内核的结构体大小 (IPv4=16, IPv6=28)
func addrToSockaddr(addr net.Addr, saBuf []byte) (saLen int, err error) {
	// 1. 强转为 *net.UDPAddr (代理/UDP 转发场景下几乎全是 UDPAddr)
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return 0, ErrUnsupportedAddr
	}

	ip := udpAddr.IP
	port := udpAddr.Port

	// 2. 自动判定 IPv4 还是 IPv6
	if ip4 := ip.To4(); ip4 != nil {
		// IPv4 对应内核的 struct sockaddr_in (大小为 16 字节)
		if len(saBuf) < 16 {
			return 0, ErrBufferTooSmall
		}
		saLen = 16

		// 强行把 saBuf 的前 16 字节解释为内核的 RawSockaddrInet4 结构体
		rsa := (*unix.RawSockaddrInet4)(unsafe.Pointer(&saBuf[0]))

		// 严格按照 Linux 内核的大端序 (Big Endian) 填充字段
		rsa.Family = unix.AF_INET
		// 端口号：转换为网络字节序 (大端)
		rsa.Port = uint16(port<<8) | uint16(port>>8)
		// IP 地址：直接拷贝 4 个字节
		copy(rsa.Addr[:], ip4)

	} else if ip6 := ip.To16(); ip6 != nil {
		// IPv6 对应内核的 struct sockaddr_in6 (大小为 28 字节)
		if len(saBuf) < 28 {
			return 0, ErrBufferTooSmall
		}
		saLen = 28

		// 强行把 saBuf 的前 28 字节解释为内核的 RawSockaddrInet6 结构体
		rsa := (*unix.RawSockaddrInet6)(unsafe.Pointer(&saBuf[0]))

		rsa.Family = unix.AF_INET6
		rsa.Port = uint16(port<<8) | uint16(port>>8)
		// IPv6 还有两个特殊的 Scope 字段，默认填 0 即可
		rsa.Flowinfo = 0
		rsa.Scope_id = 0
		// IP 地址：直接拷贝 16 个字节
		copy(rsa.Addr[:], ip6)

	} else {
		return 0, ErrUnsupportedAddr
	}

	return saLen, nil
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

// convertDeadline 将泛型原子指针 atomic.Pointer[time.Time] 转换为内核的 unix.Timespec。
// 完全消灭了类型断言，依然是完美的 0 内存分配 (0 B/op) 且符合内联标准。
func convertDeadline(p *atomic.Pointer[time.Time]) (ts unix.Timespec, hasTimeout bool, err error) {
	// 1. 直接 Load 拿到 *time.Time 强类型指针
	tPtr := p.Load()
	if tPtr == nil || tPtr.IsZero() {
		return unix.Timespec{}, false, nil
	}

	// 2. 计算相对剩余时间 (解引用获取 time.Time 对象)
	d := time.Until(*tPtr)
	if d <= 0 {
		// 已经超时，立刻拦截
		return unix.Timespec{}, false, unix.ETIMEDOUT
	}

	// 3. 数学转换
	return unix.NsecToTimespec(d.Nanoseconds()), true, nil
}
