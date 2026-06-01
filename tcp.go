package goiouring

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type TCPURingConn struct {
	*UringConn
}

type uringRawConn struct {
	u *TCPURingConn
}

// Control 执行一个用户定义的函数，并传入底层的 fd
func (rc *uringRawConn) Control(f func(uintptr)) error {
	// 这里的逻辑参考标准库：在执行期间要保证 fd 不被关闭
	f(uintptr(rc.u.fd))
	return nil
}

// Read 和 Write 在 RawConn 接口中通常用于配合非阻塞轮询，
// 在 io_uring 这种 Proactor 模型下，这两个方法通常返回错误或不实现。
func (rc *uringRawConn) Read(f func(uintptr) bool) error {
	return fmt.Errorf("Read is not supported on uringRawConn")
}

func (rc *uringRawConn) Write(f func(uintptr) bool) error {
	return fmt.Errorf("Write is not supported on uringRawConn")
}

// SetKeepAlive 设置 TCP 连接的 KeepAlive 选项。If udp, it returns an error.
func (u *UringConn) SetKeepAlive(keepalive bool) error {
	// 1. 检查是否为 UDP
	if u.isUDP() {
		return u.wrapProtocolError("SetKeepAlive")
	}

	// 2. TCP 逻辑
	var v int
	if keepalive {
		v = 1
	}
	if err := unix.SetsockoptInt(int(u.fd), unix.SOL_SOCKET, unix.SO_KEEPALIVE, v); err != nil {
		return u.wrapError("SetKeepAlive", "tcp", os.NewSyscallError("setsockopt", err))
	}
	return nil
}

// SetLinger (仅对 TCP 生效)
func (u *TCPURingConn) SetLinger(sec int) error {
	// 1. 检查是否为 UDP
	if u.isUDP() {
		return u.wrapProtocolError("SetLinger")
	}

	var l unix.Linger
	if sec >= 0 {
		l.Onoff = 1
		l.Linger = int32(sec)
	} else {
		l.Onoff = 0
		l.Linger = 0
	}
	if err := unix.SetsockoptLinger(int(u.fd), unix.SOL_SOCKET, unix.SO_LINGER, &l); err != nil {
		return u.wrapError("SetLinger", "tcp", os.NewSyscallError("SetsockoptLinger", err))
	}

	return nil
}

func (u *TCPURingConn) SetNoDelay(noDelay bool) error {
	if u.isUDP() {
		return u.wrapProtocolError("SetNoDelay")
	}
	var v int
	if noDelay {
		v = 1
	}
	// 注意：TCP_NODELAY 属于 IPPROTO_TCP 级别
	if err := unix.SetsockoptInt(int(u.fd), unix.IPPROTO_TCP, unix.TCP_NODELAY, v); err != nil {
		return u.wrapError("SetNoDelay", "tcp", os.NewSyscallError("SetsockoptInt", err))
	}
	return nil
}

func (u *TCPURingConn) SetKeepAlivePeriod(d time.Duration) error {
	if u.isUDP() {
		return u.wrapProtocolError("SetKeepAlivePeriod")
	}

	// 将 duration 转换为秒
	secs := int(d.Seconds())
	if secs <= 0 && d > 0 {
		secs = 1
	}

	// 同时设置两个 Socket 选项
	if err := unix.SetsockoptInt(int(u.fd), unix.IPPROTO_TCP, unix.TCP_KEEPIDLE, secs); err != nil {
		return u.wrapError("SetKeepAlivePeriod", "tcp", os.NewSyscallError("SetsockoptInt", err))
	}
	if err := unix.SetsockoptInt(int(u.fd), unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, secs); err != nil {
		return u.wrapError("SetKeepAlivePeriod", "tcp", os.NewSyscallError("SetsockoptInt", err))
	}
	return nil
}

func (u *TCPURingConn) SetKeepAliveConfig(config net.KeepAliveConfig) error {
	if u.isUDP() {
		return u.wrapProtocolError("SetKeepAliveConfig")
	}

	// 1. 先设置开关
	if err := u.SetKeepAlive(config.Enable); err != nil {
		return err
	}

	if config.Enable {
		// 2. 设置 Idle (对应 TCP_KEEPIDLE)
		if config.Idle > 0 {
			secs := int(config.Idle.Seconds())
			if secs <= 0 {
				secs = 1
			}
			if err := unix.SetsockoptInt(int(u.fd), unix.IPPROTO_TCP, unix.TCP_KEEPIDLE, secs); err != nil {
				return u.wrapError("SetKeepAlivePeriod", "tcp", os.NewSyscallError("SetsockoptInt", err))
			}
		}

		// 3. 设置 Interval (对应 TCP_KEEPINTVL)
		if config.Interval > 0 {
			secs := int(config.Interval.Seconds())
			if secs <= 0 {
				secs = 1
			}
			if err := unix.SetsockoptInt(int(u.fd), unix.IPPROTO_TCP, unix.TCP_KEEPINTVL, secs); err != nil {
				return u.wrapError("SetKeepAlivePeriod", "tcp", os.NewSyscallError("SetsockoptInt", err))
			}
		}

		// 4. 设置 Count (对应 TCP_KEEPCNT)
		if config.Count > 0 {
			if err := unix.SetsockoptInt(int(u.fd), unix.IPPROTO_TCP, unix.TCP_KEEPCNT, config.Count); err != nil {
				return u.wrapError("SetKeepAlivePeriod", "tcp", os.NewSyscallError("SetsockoptInt", err))
			}
		}
	}
	return nil
}

func (u *TCPURingConn) CloseRead() error {
	if u.isUDP() {
		return u.wrapProtocolError("CloseRead")
	}
	if err := unix.Shutdown(int(u.fd), unix.SHUT_RD); err != nil {
		return u.wrapError("CloseRead", "tcp", os.NewSyscallError("Shutdown", err))
	}
	return nil
}

func (u *TCPURingConn) CloseWrite() error {
	if u.isUDP() {
		return u.wrapProtocolError("CloseWrite")
	}
	if err := unix.Shutdown(int(u.fd), unix.SHUT_WR); err != nil {
		return u.wrapError("CloseWrite", "tcp", os.NewSyscallError("Shutdown", err))
	}

	return nil
}

// SyscallConn 返回底层网络连接的原始系统连接。
// 这使得 UringConn 能够完全兼容那些需要获取 FD 进行额外设置的第三方库。
func (u *TCPURingConn) SyscallConn() (syscall.RawConn, error) {
	// 如果连接已关闭，应该返回错误
	// 这里简单检查一下 fd
	if u.fd < 0 {
		return nil, os.ErrClosed
	}
	return &uringRawConn{u: u}, nil
}

// MultipathTCP 报告该连接是否正在使用 MPTCP。
func (u *TCPURingConn) MultipathTCP() (bool, error) {
	if u.isUDP() {
		return false, u.wrapProtocolError("MultipathTCP")
	}

	// 检查内核是否通过 TCP_IS_MPTCP 选项认为这是一个 MPTCP 连接
	// 注意：该常量在旧版 unix 包中可能不存在，建议直接用数值 42 或检查 unix 库版本
	// 只有在创建 Socket 时指定了协议，这里才会返回 1
	val, err := unix.GetsockoptInt(int(u.fd), unix.IPPROTO_TCP, 42) // 42 是 TCP_IS_MPTCP
	if err != nil {
		// 如果内核不支持该选项，通常返回 ENOPROTOOPT，此时应返回 false, nil
		if errors.Is(err, unix.ENOPROTOOPT) || errors.Is(err, unix.ENOTSUP) {
			return false, nil
		}
		return false, u.wrapError("MultipathTCP", "tcp", os.NewSyscallError("getsockopt", err))
	}

	return val == 1, nil
}

// WriteTo 将连接中的所有数据写入 w，直到连接关闭或发生错误。
// 这是对 io.WriterTo 接口的实现，常被 io.Copy 调用以触发零拷贝。
func (u *TCPURingConn) WriteTo(w io.Writer) (n int64, err error) {
	// 1. 尝试获取目标的 FD
	type fdGetter interface{ Fd() uintptr }
	dstObj, ok := w.(fdGetter)
	if !ok {
		// 如果目标不是 FD 类型的（比如 bytes.Buffer），回退到标准 io.Copy
		// 这里的 struct{ io.Reader }{u} 是为了避免 io.Copy 再次触发本函数导致死循环
		return io.Copy(struct{ io.Writer }{w}, struct{ io.Reader }{u})
	}
	dstFd := int(dstObj.Fd())

	// 2. 创建一个中间管道（Pipe）用于 Splice
	// io_uring 的 splice 必须经过 pipe，这是内核 zero-copy 的中转站
	var fds [2]int
	if err := unix.Pipe2(fds[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		return 0, os.NewSyscallError("pipe2", err)
	}
	pr, pw := fds[0], fds[1]
	defer unix.Close(pr)
	defer unix.Close(pw)

	var written int64
	for {
		// 第一步：从 u.fd (Socket) Splice 到 pw (Pipe Write)
		// 尝试读取最大 1MB（或管道默认限制）
		n1, err := u.splice(u.fd, int32(pw), 1024*1024)
		if err != nil {
			// 如果连接已关闭，splice 可能返回这些错误，视为 EOF
			if errors.Is(err, io.EOF) || errors.Is(err, unix.EBADF) || errors.Is(err, unix.ENOTCONN) {
				break
			}
			return written, err
		}
		if n1 == 0 {
			break
		}

		// 第二步：从 pr (Pipe Read) Splice 到 dstFd (Target)
		// 必须把刚才进管道的数据全部导出来
		left := n1
		for left > 0 {
			n2, err := u.splice(int32(pr), int32(dstFd), uint32(left))
			if err != nil {
				return written, err
			}
			left -= n2
			written += int64(n2)
		}
	}
	return written, nil
}

// splice 是对 IORING_OP_SPLICE 的底层封装
// 建议参数统一用 int，方便与 Go 标准库（如 Pipe, File.Fd()）对接
func (u *TCPURingConn) splice(inFd, outFd int32, nbytes uint32) (int, error) {
	id := atomic.AddUint64(&u.manager.requestID, 1)
	ch := readResultChanPool.Get().(chan Result)
	u.manager.pending.Store(id, &requestCtx{resCh: ch})

	u.manager.mu.Lock()
	sqes := u.getSQEsBlocking(1)
	sqe := sqes[0]

	// 关键修正：直接透传 inFd 和 outFd，不要在 fill 函数里写死 u.fd
	// 这样 WriteTo 调用 splice(u.fd, pipeW, ...)
	// 而 ReadFrom 调用 splice(pipeR, u.fd, ...) 都能正常工作
	u.fillSpliceSQE(sqe, inFd, outFd, nbytes, id)

	if err := u.manager.ring.Submit(1); err != nil {
		u.manager.mu.Unlock()
		u.manager.pending.Delete(id)
		return 0, err
	}
	u.manager.mu.Unlock()

	res := <-ch
	readResultChanPool.Put(ch)

	if res.Err != nil {
		return 0, res.Err
	}

	// 修正：res.N 是 int32，强转为 int 返回
	return int(res.N), nil
}

// ReadFrom 从 r 中读取数据并写入到 u 的连接中，直到 r 到达 EOF。
func (u *TCPURingConn) ReadFrom(r io.Reader) (n int64, err error) {
	type fdGetter interface{ Fd() uintptr }
	srcObj, ok := r.(fdGetter)
	if !ok {
		// 如果来源没有 FD，回退到普通 io.Copy
		return io.Copy(struct{ io.Writer }{u}, r)
	}
	srcFd := int(srcObj.Fd())

	// 准备 Pipe
	var fds [2]int
	if err := unix.Pipe2(fds[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		return 0, os.NewSyscallError("pipe2", err)
	}
	pr, pw := fds[0], fds[1]
	defer unix.Close(pr)
	defer unix.Close(pw)

	var readTotal int64
	for {
		// 1. 从源 srcFd Splice 到管道进入端 pw
		n1, err := u.splice(int32(srcFd), int32(pw), 1024*1024)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return readTotal, err
		}
		if n1 == 0 {
			break
		}

		// 2. 从管道退出端 pr Splice 到 u.fd (Socket)
		left := n1
		for left > 0 {
			n2, err := u.splice(int32(pr), int32(u.fd), uint32(left))
			if err != nil {
				return readTotal, err
			}
			left -= n2
			readTotal += int64(n2)
		}
	}
	return readTotal, nil
}

// fillSpliceSQE 严格按照内核 io_uring_sqe 布局填充，并设置用于异步回调的 UserData
func (u *TCPURingConn) fillSpliceSQE(sqe *SQE, inFd, outFd int32, nbytes uint32, id uint64) {
	// 1. 设置异步标识 (必须设置，否则 Manager 收不到通知)
	sqe.UserData = id

	// 2. 基本操作码
	sqe.Opcode = IORING_OP_SPLICE

	// 3. 源文件描述符
	sqe.Fd = inFd

	// 4. 偏移量设置 (全部使用 -1)
	sqe.Off = ^uint64(0)  // 告诉内核: "不要使用我给出的偏移量，请直接使用该文件描述符当前的读写位置"
	sqe.Addr = ^uint64(0) // 告诉内核: "内核会直接从流的当前位置（当前的 buffer）读取"

	// 5. 传输长度与标志
	sqe.Len = nbytes
	sqe.RWFlags = 0 // 对应 splice_flags

	// 6. 目标文件描述符
	sqe.SpliceFdIn = outFd

	// 7. 清理其他字段
	sqe.Ioprio = 0
	sqe.Flags = 0
	sqe.BufIndex = 0
	sqe.Personality = 0
}
