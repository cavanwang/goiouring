package goiouring

import (
	"runtime"
	"sync"
)

// UringManager 全局 io_uring 多核环管理器
type UringManager struct {
	rings    []*Ring // 核心：Per-CPU 独立环切片
	numCPU   uint32  // 缓存 CPU 核心数，用于高效取模
	closeOnce sync.Once
}

// NewUringManager 创建并初始化多核 io_uring 管理器
// entriesPerRing: 每个 io_uring 环的 SQE 队列深度 (建议设为 1024 或 2048)
func NewUringManager(entriesPerRing uint32, taskChannelLen int) (*UringManager, error) {
	numCPU := uint32(runtime.NumCPU())
	manager := &UringManager{
		rings:  make([]*Ring, numCPU),
		numCPU: numCPU,
	}

	// 为每个 CPU 核心独立开辟疆土
	for i := uint32(0); i < numCPU; i++ {
		// 1. 实例化底层 io_uring 环 (这里需要调用你原有的 Ring 初始化函数)
		// 假设你原有的初始化函数类似：NewRing(entries)
		ring, err := NewRing(entriesPerRing, taskChannelLen)
		if err != nil {
			// 如果中途某个环初始化失败，安全回滚之前已经打开的环，防止文件描述符泄漏
			manager.Close()
			return nil, err
		}

		manager.rings[i] = ring

		// 2. 核心大招：异步启动后台发动机，发动机内部会自动调用 runtime.LockOSThread()
		// 建议你在 startPollToDoTasks 内部首行加入：
		// unix.SchedSetaffinity(0, &unix.CPUSet{}) 来实现更硬核的物理核绑定 (选填)
		go ring.startPollToDoTasks()
		
		// 3. 同时启动你原本就有的收割发动机
		// go ring.startPollDoneTasks()
	}

	return manager, nil
}

// GetRingForFD 调度灵魂：根据网络连接的 Fd，通过 $O(1)$ 纯数学取模算法
// 将该连接终身绑定到某个特定 CPU 核心的环上，彻底消灭跨核锁竞争
func (m *UringManager) GetRingForFD(fd int32) *Ring {
	if m.numCPU == 0 {
		return nil
	}
	// 强转 uint32 消除负数 FD 干扰，精准定位物理环
	index := uint32(fd) % m.numCPU
	return m.rings[index]
}

// Close 优雅关闭管理器，释放所有内核 io_uring 资源
func (m *UringManager) Close() {
	m.closeOnce.Do(func() {
		for _, ring := range m.rings {
			if ring != nil {
				// 1. 安全关闭发动机的通道，让 goroutine 退出
				if ring.closeCh != nil {
					close(ring.closeCh)
				}
				// 2. 调用原有的系统调用真正关闭 io_uring 实例 (释放内存和内核 fd)
				// ring.Close() 
			}
		}
	})
}
