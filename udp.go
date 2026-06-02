package goiouring

import (
	"io"
	"net"
	"runtime"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

var (
	// 全局的 IOTask 对象池 (针对 UDP 场景定制)
	udpReadTaskPool = sync.Pool{
		New: func() interface{} {
			return &IOTask{
				ResChan: make(chan int, 1),
				// 预分配足够的空间存放 sockaddr，避免每次都分配内存
				SockAddr: make([]byte, 128),
			}
		},
	}
)

func (u *UringConn) ReadFrom(b []byte) (n int, addr net.Addr, err error) {
	if len(b) == 0 {
		return 0, nil, nil
	}

	// 1. 从对象池捞取专用的接收 Task
	task := udpReadTaskPool.Get().(*IOTask)
	task.Err = nil
	task.HasTimeout = 0
	task.Buf = b // 直接引用用户传入的接收切片

	// 2. 处理硬件超时 (调用之前封装好的 atomic.Pointer[time.Time] 转换小函数)
	ts, hasTimeout, tErr := convertDeadline(&u.readDeadline)
	if tErr != nil {
		udpReadTaskPool.Put(task)
		return 0, nil, tErr
	}
	if hasTimeout {
		task.HasTimeout = 1
		task.Timespec = ts
	}

	// 3. 组装完美的 Msghdr（全部挂载在 task 实体上，防御栈扩容导致的地址漂移）
	task.Iov.Base = &task.Buf[0]
	task.Iov.Len = uint64(len(b))

	// 绑定 task 身上稳固的 RawSa 内存块，强行指定最大可用长度
	task.Msg.Name = (*byte)(unsafe.Pointer(&task.RawSa))
	task.Msg.Namelen = uint32(unix.SizeofSockaddrAny)
	task.Msg.Iov = &task.Iov
	task.Msg.Iovlen = 1
	task.Msg.Control = nil
	task.Msg.Controllen = 0
	task.Msg.Flags = 0

	// 4. 基础字段初始化
	task.OpCode = IORING_OP_RECVMSG // 使用接收专用网络特种兵操作码
	task.Fd = int32(u.fd)

	// 5. 【终极无锁推包】直接甩给后台的 startPollToDoTasks 发动机
	u.r.PushTask(task)

	// 6. 挂起当前业务协程，静静等待 startPollDoneTasks 收割内核交货通知
	bytesRead := <-task.ResChan
	resErr := task.Err

	// 7. 【生命周期护城河】在读取到结果前，强行禁止编译器对 task 进行重排或释放
	runtime.KeepAlive(task)

	// 8. 判定内核结果
	if resErr != nil {
		udpReadTaskPool.Put(task)
		return 0, nil, resErr
	}
	if bytesRead == 0 {
		udpReadTaskPool.Put(task)
		return 0, nil, io.EOF
	}

	// 9. 解析对端物理套接字地址（此时 task.RawSa 已被内核 DMA 强行改写为真实的远端 IP/Port）
	netAddr, _ := parseSockaddr(&task.RawSa)

	// 10. 资源回收归还
	udpReadTaskPool.Put(task)

	return bytesRead, netAddr, nil
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
