//go:build linux

package goiouring

import (
	"fmt"
	"math/bits"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// --- Linux 内核 io_uring 系统调用与常量定义 (x86_64) ---
const (
	SYS_IO_URING_SETUP = 425
	SYS_IO_URING_ENTER = 426

	IORING_OFF_SQ_RING = 0
	IORING_OFF_CQ_RING = 0x8000000
	IORING_OFF_SQES    = 0x10000000

	IORING_FEAT_SINGLE_MMAP = 1 << 0
	IORING_ENTER_GETEVENTS  = 1 << 0
	IORING_OP_LINK_TIMEOUT  = 15 // 新增：超时操作码
	IORING_OP_SPLICE        = 31 // 新增：Splice 操作码

	// SQPOLL 相关标志
	IORING_SETUP_SQPOLL    = 1 << 1 // 启用内核线程轮询
	IORING_SQ_NEED_WAKEUP  = 1 << 0 // SQ 环 Flags 标志：内核线程已休眠，需唤醒
	IORING_ENTER_SQ_WAKEUP = 1 << 1 // Enter 标志：唤醒内核线程

	// 支持的常用异步操作码
	IORING_OP_SEND    = 26 // 针对已连接 Socket 的专用发送（相当于标准 BSD 的 send()）
	IORING_OP_READ    = 22
	IORING_OP_SENDMSG = 14 // UDP 高性能异步发送核心操作码
	IORING_OP_RECVMSG = 17

	IOSQE_FIXED_FILE     = 1 << 0 // 1
	IOSQE_IO_DRAIN       = 1 << 1 // 2
	IOSQE_IO_LINK        = 1 << 2 // 将当前 SQE 与下一个 SQE 强链锁绑定（超时必加）
	IOSQE_IO_HARDLINK    = 1 << 3 // 8
	IORING_ENTER_EXT_ARG = 1 << 3 // 告诉内核我们传入了扩展的时间参数
)

const (
	// 攒批触发系统调用的阈值
	batchSize = 64
)

// --- 内存布局对齐结构体 (严格匹配 Linux 内核) ---

type IOUringParams struct {
	SqEntries, CqEntries, Flags, SqThreadCpu, SqThreadIdle, Features uint32
	WqFd                                                             uint32
	Resv                                                             [3]uint32
	SqOff                                                            ioSqringOffsets
	CqOff                                                            ioCqringOffsets
}

type ioSqringOffsets struct {
	Head, Tail, RingMask, RingEntries, Flags, Dropped, Array uint32
	Resv                                                     [3]uint32
}

type ioCqringOffsets struct {
	Head, Tail, RingMask, RingEntries, Overflow, Cqes, Flags uint32
	Resv                                                     [3]uint32
}

type SQE struct {
	Opcode uint8
	Flags  uint8
	Ioprio uint16
	Fd     int32

	Off      uint64
	Addr     uint64
	Len      uint32
	RWFlags  uint32 // 必须有这个，确保 UserData 不会产生意外 Padding
	UserData uint64

	// 联合体部分：这里必须凑够剩下的字节，确保整个结构体为 64 字节
	// 剩下的部分是：buf_index(2) + personality(2) + splice_fd_in(4) + addr3[2](16) = 24 字节
	BufIndex    uint16
	Personality uint16
	SpliceFdIn  int32
	_pad2       [2]uint64
}

type CQE struct {
	UserData uint64
	Res      int32
	Flags    uint32
}

// --- 1. 工业级 IOTask 异步/同步转换桥梁 ---

type IOTask struct {
	OpCode uint8  // IORING_OP_READ 或 IORING_OP_WRITE
	Fd     int32  // 网络连接或文件的 fd
	Buf    []byte // 目标内存缓冲区 (直接暴露给内核执行 DMA)
	Offset uint64 // 文件偏移量 (网络连接填 0)

	// 核心纽带：容量必须为 1 的缓冲 Channel
	// 当内核完成 I/O 后，PollDoneTasks 会往这里写入结果，瞬间唤醒挂起的业务协程
	ResChan chan int
	Err     error // 存放内核返回的错误原因

	// --- UDP 专用特化字段 ---
	Iov      unix.Iovec          // 持久化 iovec 结构体，防止进入内核后被栈释放
	Msg      unix.Msghdr         // 持久化 msghdr 结构体
	SockAddr []byte              // 持久化通用套接字地址 (addrToSockaddr 转换出的 raw 数据)
	RawSa    unix.RawSockaddrAny // 专供内核异步回写对端来源 IP 和端口的物理槽位，防止栈扩容漂移

	// --- 超时特化字段 ---
	HasTimeout uint8         // 0: 无超时, 1: 有超时
	Timespec   unix.Timespec // 独立持久化超时时间，防止多线程踩踏
}

type Ring struct {
	fd     int
	params IOUringParams

	// SQ 环：仅限全局唯一的 startPollToDoTasks 协程读写，彻底告别多线程 atomic 锁竞争
	sqPtr   []byte
	sqHead  *uint32 // 内核写，用户读
	sqTail  *uint32 // 用户写，内核读
	sqMask  *uint32
	sqArray []uint32
	sqes    []SQE

	localTail uint32 // startPollToDoTasks 独占的局部尾指针，用于无锁快速计算位置

	// CQ 环：仅限全局唯一的 startPollPollDoneTasks 协程读写
	cqPtr  []byte
	cqHead *uint32 // 用户写，内核读
	cqTail *uint32 // 内核写，用户读
	cqMask *uint32
	cqes   []CQE

	// 用户态的高效通信管道
	jobChan chan *IOTask
	closeCh chan struct{}
}

// 🔥 【新增】严格对齐 Linux 内核的 io_uring_getevents_arg 结构体
type ioUringGeteventsArg struct {
	Sigmask   uint64
	SigmaskSz uint32
	Pad       uint32
	Ts        uint64 // 指向 unix.Timespec 的用户态物理指针
}

func init() {
	// 强行在编译/初始化阶段校验结构体大小，防止 32/64 位对齐产生 Padding 导致内核读错内存
	if unsafe.Sizeof(SQE{}) != 64 {
		panic("[Critical] io_uring SQE struct size alignment error: expected 64")
	}
	if unsafe.Sizeof(CQE{}) != 16 {
		panic("[Critical] io_uring CQE struct size alignment error: expected 16")
	}
}

// NewRing 创建并初始化一个具有高性能双雄协程的 io_uring 引擎
func NewRing(ringEntries uint32, taskChannelLen int) (*Ring, error) {
	ringEntries = nextPowerOfTwo(ringEntries)

	var p IOUringParams
	fd, _, errno := unix.RawSyscall(SYS_IO_URING_SETUP, uintptr(ringEntries), uintptr(unsafe.Pointer(&p)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup error: %v", errno)
	}

	r := &Ring{
		fd:      int(fd),
		params:  p,
		jobChan: make(chan *IOTask, taskChannelLen), // 缓冲管道，承载来自上游几百个业务协程的并发
		closeCh: make(chan struct{}),
	}

	// 1. 映射 SQ 内存
	sqSize := p.SqOff.Array + p.SqEntries*4
	sqPtr, err := unix.Mmap(r.fd, IORING_OFF_SQ_RING, int(sqSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		unix.Close(r.fd)
		return nil, err
	}
	r.sqPtr = sqPtr
	r.sqHead = (*uint32)(unsafe.Pointer(&sqPtr[p.SqOff.Head]))
	r.sqTail = (*uint32)(unsafe.Pointer(&sqPtr[p.SqOff.Tail]))
	r.sqMask = (*uint32)(unsafe.Pointer(&sqPtr[p.SqOff.RingMask]))
	r.sqArray = (*[1 << 28]uint32)(unsafe.Pointer(&sqPtr[p.SqOff.Array]))[:p.SqEntries]

	// 2. 映射 SQE 结构体数组
	sqeSize := p.SqEntries * 64
	sqePtr, err := unix.Mmap(r.fd, IORING_OFF_SQES, int(sqeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = unix.Munmap(sqPtr)
		_ = unix.Close(r.fd)
		return nil, err
	}
	r.sqes = (*[1 << 26]SQE)(unsafe.Pointer(&sqePtr[0]))[:p.SqEntries]

	// 3. 映射 CQ 内存 (判断内核是否支持 Single MMAP 优化)
	if p.Features&IORING_FEAT_SINGLE_MMAP != 0 {
		r.cqPtr = sqPtr
	} else {
		cqSize := p.CqOff.Cqes + p.CqEntries*16
		cqPtr, err := unix.Mmap(r.fd, IORING_OFF_CQ_RING, int(cqSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
		if err != nil {
			_ = unix.Munmap(sqPtr)
			_ = unix.Munmap(sqePtr)
			_ = unix.Close(r.fd)
			return nil, err
		}
		r.cqPtr = cqPtr
	}
	r.cqHead = (*uint32)(unsafe.Pointer(&r.cqPtr[p.CqOff.Head]))
	r.cqTail = (*uint32)(unsafe.Pointer(&r.cqPtr[p.CqOff.Tail]))
	r.cqMask = (*uint32)(unsafe.Pointer(&r.cqPtr[p.CqOff.RingMask]))
	r.cqes = (*[1 << 27]CQE)(unsafe.Pointer(&r.cqPtr[p.CqOff.Cqes]))[:p.CqEntries]

	// 初始化局部尾指针快照
	r.localTail = *r.sqTail

	// --- 异步双雄并驾齐驱 ---
	go r.startPollToDoTasks() // 优化更名：统一待提交任务管道轮询与攒批下发发动机
	go r.startPollDoneTasks() // 优化更名：统一已完成任务结果收割与分发器

	return r, nil
}

// PushTask 供上游业务 Goroutine 并发调用的非阻塞推包接口
func (r *Ring) PushTask(task *IOTask) {
	r.jobChan <- task
}

// --- 2. 核心发动机：单一协程掌控的 startPollToDoTasks (结合 Channel 和 Timer) ---
func (r *Ring) startPollToDoTasks() {
	// 将当前协程死死绑定在一个固定的内核线程上，极大加速系统调用进出效率
	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()

	const tickInterval = 100 * 1000 // 100微秒超时兜底
	var unsubmitted uint32

	timer := unsafeNewTimer(tickInterval)
	defer timer.Stop()

	for {
		select {
		// 场景 A：待办任务管道有活干 (上游业务协程塞包进来了)
		case task := <-r.jobChan:
			// 1. 【修复】动态计算本次任务需要占据几个 SQE 槽位 (普通为 1，带超时则为 2)
			numSQEs := uint32(1)
			if task.HasTimeout == 1 {
				numSQEs = 2
			}

			// 2. 【修复】精密环满检查：必须确保环内至少有满足 numSQEs 的空闲槽位
			head := atomic.LoadUint32(r.sqHead)
			if r.localTail+numSQEs-head > r.params.SqEntries {
				// 环空间不够放当前整套任务了，强行把手里的存货 Flush 进内核提货
				if unsubmitted > 0 {
					r.flushAndEnter(unsubmitted)
					unsubmitted = 0
					unsafeResetTimer(timer, tickInterval)
				}
				// 暂时让出 P，给内核一点时间通过 DMA 消耗任务
				runtime.Gosched()
				head = atomic.LoadUint32(r.sqHead)
				if r.localTail+numSQEs-head > r.params.SqEntries {
					// 依然满则发起 0 提交系统调用进行强行阻尼
					r.flushAndEnter(0)
				}
			}

			// 3. 纯内存、无锁分配主任务 SQE 槽位
			index := r.localTail & *r.sqMask
			r.sqArray[index] = index
			sqe := &r.sqes[index]

			// 初始化主 SQE 的通用字段
			sqe.Opcode = task.OpCode
			sqe.Fd = task.Fd
			sqe.Off = task.Offset
			sqe.UserData = uint64(uintptr(unsafe.Pointer(task)))
			sqe.Flags = 0 // 【修复】必须显式初始化 Flags

			// 4. 【修复】核心微观分流：区分 TCP(字节流) 与 UDP(Msghdr套接字) 的内存物理拓扑
			if task.OpCode == IORING_OP_READ || task.OpCode == IORING_OP_SEND {
				// TCP / 文件：Addr 指向原始字节切片缓冲区
				sqe.Addr = uint64(uintptr(unsafe.Pointer(&task.Buf[0])))
				sqe.Len = uint32(len(task.Buf))
			} else {
				// UDP (SENDMSG / RECVMSG)：Addr 指向 task 身上的核心 Msghdr 物理底座
				sqe.Addr = uint64(uintptr(unsafe.Pointer(&task.Msg)))
				sqe.Len = 1 // 根据 Linux io_uring 规范，SENDMSG/RECVMSG 的 len 固定填 1
			}

			r.localTail++
			unsubmitted++

			// 5. 【新增】超时链接处理器：如果是带超时的请求，紧挨着塞入第二个超时链接 SQE
			if task.HasTimeout == 1 {
				// 核心防线：打上锁链标志，告诉内核当前主操作如果超时未动，直接由下面的超时操作接管斩断
				sqe.Flags |= IOSQE_IO_LINK

				nextIndex := r.localTail & *r.sqMask
				r.sqArray[nextIndex] = nextIndex
				tsqe := &r.sqes[nextIndex]

				// 严格初始化内核级超时联动结构体
				tsqe.Opcode = IORING_OP_LINK_TIMEOUT
				tsqe.Fd = -1                                                // 固定填 -1
				tsqe.Addr = uint64(uintptr(unsafe.Pointer(&task.Timespec))) // 指向 task 身上的相对剩余时间
				tsqe.Len = 1                                                // 代表 1 个 timespec 结构体
				tsqe.Off = 0
				tsqe.UserData = 0 // 超时触发本身内核会自动熔断前一个主 SQE，这里不需要给完成环回执，填 0 即可
				tsqe.Flags = 0

				r.localTail++
				unsubmitted++
			}

			// 6. 攒批数量达到黄金阈值 64，立刻批量进内核触发真正的硬件 DMA
			if unsubmitted >= batchSize {
				r.flushAndEnter(unsubmitted)
				unsubmitted = 0
				unsafeResetTimer(timer, tickInterval)
			}

		// 场景 B：超时兜底机制触发 (流量低谷期，手里攒了几个任务但不够64个)
		case <-timer.C:
			if unsubmitted > 0 {
				r.flushAndEnter(unsubmitted)
				unsubmitted = 0
			}
			unsafeResetTimer(timer, tickInterval)

		// 场景 C：网关关闭
		case <-r.closeCh:
			return
		}
	}
}

// flushAndEnter 统一的指针合拢与唯一系统调用下发入口
func (r *Ring) flushAndEnter(toSubmit uint32) {
	if toSubmit > 0 {
		// 阻断编译器优化重排，确保内核看到 sqTail 推进前，SQE 各字段已被全刷入 CPU Cache Line
		runtime.KeepAlive(r.sqes)
		// 纯单线程内存操作推进物理尾指针，消灭多核心多线程 CAS 造成的 Cache 踩踏
		*r.sqTail = r.localTail
	}

	// 触发整个转发链路唯一的内核系统调用
	_, _, _ = unix.Syscall6(
		unix.SYS_IO_URING_ENTER,
		uintptr(r.fd),
		uintptr(toSubmit),
		0, // min_complete = 0 (ToDoTasks 只管发，不管等，收割任务交给专职的 DoneTasks 协程)
		0, // flags
		0, 0,
	)
}

// --- 3. 接收端：单一协程掌控的 startPollDoneTasks (O(1) 指针转回与解耦唤醒) ---
func (r *Ring) startPollDoneTasks() {
	// 保持解除 LockOSThread 状态，让 Go 弹性调度

	// 💡 彻底抛弃内核高频 timespec 结构体，消灭内核定时器开销
	for {
		head := atomic.LoadUint32(r.cqHead)
		tail := atomic.LoadUint32(r.cqTail)

		// 如果完成环空空如也，说明当前没有已完成的 I/O 事件
		if head == tail {
			// 发起一个特殊的 enter 系统调用：不提交任务(0)，但要求至少卡住等 1 个完成事件(1)
			// 当前 DoneTasks 线程会进入内核挂起态，不消耗任何业务 CPU 算力
			_, _, errno := unix.Syscall6(
				unix.SYS_IO_URING_ENTER,
				uintptr(r.fd),
				0, // to_submit = 0
				1, // min_complete = 1
				IORING_ENTER_GETEVENTS,
				0, 0,
			)
			if errno != 0 && errno != unix.EAGAIN && errno != unix.EINTR {
				select {
				case <-r.closeCh:
					return
				default:
					continue
				}
			}
			continue
		}

		// 批处理收割逻辑（保持你的快照收割）
		for head != tail {
			index := head & *r.cqMask
			cqe := r.cqes[index]

			// O(1) 终极绝技：把内核吐出来的 64 位 UserData 直接强转回 *IOTask 结构体指针
			// 彻底干掉全局 Map 查找和对应的读写锁竞争
			task := (*IOTask)(unsafe.Pointer(uintptr(cqe.UserData)))

			// 提取结果并无缝分发
			if cqe.Res < 0 {
				task.Err = unix.Errno(-cqe.Res) // 内核负数代表标准错误码
				task.ResChan <- 0
			} else {
				task.ResChan <- int(cqe.Res) // 塞入成功读取/写入的实际字节数，上游业务协程瞬间苏醒！
			}

			head++
			tail = atomic.LoadUint32(r.cqTail)
		}

		// 统一对内核上报消费进度，腾出 CQ 槽位
		atomic.StoreUint32(r.cqHead, head)
	}
}

// --- 4. 辅助配套工具函数 (正统标准库扩展) ---

func (r *Ring) Close() {
	close(r.closeCh)
	_ = unix.Munmap(r.sqPtr)
	if &r.cqPtr[0] != &r.sqPtr[0] {
		_ = unix.Munmap(r.cqPtr)
	}
	_ = unix.Close(r.fd)
}

func nextPowerOfTwo(n uint32) uint32 {
	if n <= 1 {
		return 1
	}
	if n&(n-1) == 0 {
		return n
	}
	return 1 << bits.Len32(n)
}

func unsafeNewTimer(ns int64) *time.Timer {
	return time.NewTimer(time.Duration(ns))
}

func unsafeResetTimer(timer *time.Timer, ns int64) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(time.Duration(ns))
}
