//go:build linux

package common

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// --- 内核常量定义 (x86_64) ---
const (
	SYS_IO_URING_SETUP = 425
	SYS_IO_URING_ENTER = 426

	IORING_OFF_SQ_RING = 0
	IORING_OFF_CQ_RING = 0x8000000
	IORING_OFF_SQES    = 0x10000000

	IORING_FEAT_SINGLE_MMAP = 1 << 0
	IORING_ENTER_GETEVENTS  = 1 << 0

	// SQPOLL 相关标志
	IORING_SETUP_SQPOLL    = 1 << 1 // 启用内核线程轮询
	IORING_SQ_NEED_WAKEUP  = 1 << 0 // SQ 环 Flags 标志：内核线程已休眠，需唤醒
	IORING_ENTER_SQ_WAKEUP = 1 << 1 // Enter 标志：唤醒内核线程

	// 常用操作码
	IORING_OP_READ         = 22
	IORING_OP_WRITE        = 23
	IORING_OP_LINK_TIMEOUT = 15 // 新增：超时操作码

	IORING_OP_RECVMSG = 17
	IORING_OP_SENDMSG = 16
)

// --- 结构体定义 ---
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
	Opcode   uint8
	Flags    uint8
	Ioprio   uint16
	Fd       int32
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

type Ring struct {
	fd     int
	params IOUringParams

	sqPtr   []byte
	sqHead  *uint32
	sqTail  *uint32
	sqMask  *uint32
	sqFlags *uint32
	sqArray []uint32
	sqes    []SQE

	cqPtr  []byte
	cqHead *uint32
	cqTail *uint32
	cqMask *uint32
	cqes   []CQE
}

func init() {
	var sqe SQE
	var cqe CQE

	// 1. 验证 SQE 总体大小 (必须 64 字节)
	sqeSize := unsafe.Sizeof(sqe)
	if sqeSize != 64 {
		panic(fmt.Sprintf("[Critical] io_uring SQE struct size alignment error: got %d, expected 64", sqeSize))
	}

	// 2. 验证关键字段偏移量 (x86_64 标准布局)
	// UserData 必须在第 32 字节（8字节对齐）
	userDataOffset := unsafe.Offsetof(sqe.UserData)
	if userDataOffset != 32 {
		panic(fmt.Sprintf("[Critical] SQE.UserData offset error: got %d, expected 32. Potential padding issue!", userDataOffset))
	}

	// 3. 验证联合体起始位置
	// BufIndex 紧跟在 UserData 之后 (32 + 8 = 40)
	bufIndexOffset := unsafe.Offsetof(sqe.BufIndex)
	if bufIndexOffset != 40 {
		panic(fmt.Sprintf("[Critical] SQE.BufIndex offset error: got %d, expected 40", bufIndexOffset))
	}

	// 4. 验证 CQE 大小 (必须 16 字节)
	cqeSize := unsafe.Sizeof(cqe)
	if cqeSize != 16 {
		panic(fmt.Sprintf("[Critical] io_uring CQE struct size alignment error: got %d, expected 16", cqeSize))
	}
}

// New 初始化 Ring
func NewRing(entries uint32) (*Ring, error) {
	var p IOUringParams
	//p.Flags = IORING_SETUP_SQPOLL
	//p.SqThreadIdle = 2000
	p.Flags = 0

	fd, _, errno := unix.RawSyscall(SYS_IO_URING_SETUP, uintptr(entries), uintptr(unsafe.Pointer(&p)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup error: %v", errno)
	}

	r := &Ring{
		fd:     int(fd),
		params: p,
	}

	// SQ 映射
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
	r.sqFlags = (*uint32)(unsafe.Pointer(&sqPtr[p.SqOff.Flags]))
	r.sqArray = (*[1 << 28]uint32)(unsafe.Pointer(&sqPtr[p.SqOff.Array]))[:p.SqEntries]

	// SQEs 映射
	sqeSize := p.SqEntries * 64
	sqePtr, err := unix.Mmap(r.fd, IORING_OFF_SQES, int(sqeSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = unix.Munmap(sqPtr)
		_ = unix.Close(r.fd)
		return nil, err
	}
	r.sqes = (*[1 << 26]SQE)(unsafe.Pointer(&sqePtr[0]))[:p.SqEntries]

	// CQ 映射
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

	return r, nil
}

// GetSQE 获取单个槽位
func (r *Ring) GetSQE() *SQE {
	sqes := r.GetSQEs(1)
	if sqes == nil {
		return nil
	}
	return sqes[0]
}

// GetSQEs 原子性地一次性预留 n 个连续槽位。
// 该函数会直接更新 sqArray，但不会推进 sqTail（由 FlushSQE 处理）。
func (r *Ring) GetSQEs(n uint32) []*SQE {
	if n == 0 {
		return nil
	}

	head := atomic.LoadUint32(r.sqHead)
	tail := *r.sqTail // 假设调用方已持有管理器级别的锁

	// 检查环是否有足够连续空间
	if tail+n-head > r.params.SqEntries {
		return nil
	}

	res := make([]*SQE, n)
	for i := uint32(0); i < n; i++ {
		// 计算当前逻辑位置对应的环索引
		// io_uring 允许逻辑 tail 持续增长，通过 mask 取模定位物理数组位置
		logicalIdx := tail + i
		index := logicalIdx & *r.sqMask

		// 1. 在 SQ 数组中建立逻辑位置到 SQE 数组索引的映射
		// 注意：此处是 io_uring 要求的关键，内核通过读取 sqArray[logicalTail & mask] 找到 SQE
		r.sqArray[index] = index

		// 2. 获取 SQE 结构体引用
		res[i] = &r.sqes[index]
	}

	return res
}

// FlushSQEs 批量更新尾指针，使内核看到新提交的 n 个任务
func (r *Ring) FlushSQEs(n uint32) {
	if n == 0 {
		return
	}

	// 【关键修复】
	// 虽然 atomic 在 x86 是全屏障，但在高性能 IO 场景下，
	// 我们需要确保在更新 Tail 之前，所有的 SQE 字段（Fd, Addr 等）
	// 已经从 CPU 寄存器刷到了能被内核线程看到的 Cache Line 中。

	// 强制编译器不在此处进行指令重排
	runtime.KeepAlive(r.sqes)

	// 更新 Tail
	atomic.AddUint32(r.sqTail, n)
}

// 修改 Submit 增加 n 参数，代表本次期望内核处理的任务数
func (r *Ring) Submit(n uint32) error {
	if r.params.Flags&IORING_SETUP_SQPOLL != 0 {
		// SQPOLL 模式下，通常不需要 Enter，除非内核线程睡着了
		if atomic.LoadUint32(r.sqFlags)&IORING_SQ_NEED_WAKEUP != 0 {
			_, _, errno := unix.RawSyscall6(
				SYS_IO_URING_ENTER,
				uintptr(r.fd),
				0, // to_submit (SQPOLL 模式下，内核自己会看，这里传0即可)
				0, // min_complete
				uintptr(IORING_ENTER_SQ_WAKEUP),
				0, 0,
			)
			// EAGAIN 和 EINTR 是正常现象，不需要作为错误返回
			if errno != 0 && errno != unix.EAGAIN && errno != unix.EINTR {
				return errno
			}
		}
		return nil
	}

	// 非 SQPOLL 模式：显式告知内核处理 n 个任务
	// 如果 n 为 0，内核也会检查环，但传 n 效率更高
	_, _, errno := unix.RawSyscall6(
		SYS_IO_URING_ENTER,
		uintptr(r.fd),
		uintptr(n), // to_submit
		0,          // min_complete (WaitCQE 里会处理这个，这里传0)
		0,
		0, 0,
	)

	if errno != 0 && errno != unix.EAGAIN && errno != unix.EINTR {
		return errno
	}
	return nil
}

// WaitCQE 阻塞等待完成事件
func (r *Ring) WaitCQE() (userData uint64, res int32, err error) {
	for {
		head := atomic.LoadUint32(r.cqHead)
		tail := atomic.LoadUint32(r.cqTail) // 必须 Load tail

		if head == tail {
			// 使用阻塞式 Syscall，允许 Go 调度器切换 G
			_, _, errno := unix.Syscall6(
				unix.SYS_IO_URING_ENTER,
				uintptr(r.fd),
				0, // to_submit
				1, // min_complete
				uintptr(IORING_ENTER_GETEVENTS),
				0, 0,
			)
			if errno != 0 && errno != unix.EAGAIN && errno != unix.EINTR {
				return 0, 0, errno
			}
			continue
		}

		// 确保读取 cqe 之前，内核对内存的写入对我们可见
		// 在 Go 中，atomic.Load 对 tail 的读取在 x86 下保证了这一点

		index := head & *r.cqMask
		cqe := r.cqes[index]

		// 拷贝数据，防止更新 head 后内存被内核覆盖
		userData = cqe.UserData
		res = cqe.Res

		// 必须使用 Atomic Write 更新 head，告知内核槽位已空
		atomic.StoreUint32(r.cqHead, head+1)

		return userData, res, nil
	}
}

func (r *Ring) Close() {
	_ = unix.Close(r.fd)
}
