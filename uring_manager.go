package goiouring

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// UringManager 管理 io_uring 实例的生命周期、资源池及结果分发。
type UringManager struct {
	ring            *Ring
	bufPool         *MultiPool
	pending         sync.Map
	requestID       uint64
	sqeWaiter       chan struct{}
	mu              sync.Mutex
	pendingCount    uint32
	maxBatchTasks   uint32
	maxWaitDuration time.Duration
}

func NewUringManager(r *Ring, p *MultiPool, maxWait time.Duration, maxBatch uint32) *UringManager {
	m := &UringManager{
		ring:            r,
		bufPool:         p,
		sqeWaiter:       make(chan struct{}, 1),
		maxWaitDuration: maxWait,
		maxBatchTasks:   maxBatch,
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

// PostRequest 负责：等待物理空间 -> 推送物理 Tail (Flush) -> 攒批提交。
// PostRequest返回表示已经成功提交了任务到内核，调用者需要自己等待任务完成。
func (m *UringManager) PostRequest(n uint32) {
	if n == 0 {
		return
	}

	for {
		m.mu.Lock()

		// 关键修正：计算真实的物理占用空间
		// head: 内核读到了哪; tail: 用户写到了哪
		head := atomic.LoadUint32(m.ring.sqHead)
		tail := atomic.LoadUint32(m.ring.sqTail)

		// (tail - head) 是环中当前已占用的物理槽位数
		// 只有加上 n 仍不超过物理容量时，才能写入
		if (tail-head)+n <= m.ring.params.SqEntries {
			break // 拿到票了，继续执行
		}

		// 2. 【关键：打破死锁】
		// 如果环满了，且我们还有没提交的任务(pendingCount > 0)
		// 必须立刻提交！否则内核永远不会释放空间。
		if m.pendingCount > 0 {
			m.ring.Submit(m.pendingCount)
			m.pendingCount = 0
		}
		// 环满了，释放锁
		m.mu.Unlock()

		// 3. 等待唤醒
		// 使用 runtime.Gosched() 或者带超时的 select
		// 最稳妥的是用 sync.Cond，它支持 Broadcast() 唤醒所有等待者
		select {
		case <-m.sqeWaiter:
		case <-time.After(m.maxWaitDuration):
			// 兜底：防止信号丢失，强制重试
		}
	}

	// 已经拿到物理空间，并持有 mu 锁
	defer m.mu.Unlock()

	// 1. 更新内存里的 Tail
	m.ring.FlushSQEs(n)
	m.pendingCount += n

	// 2. 攒批逻辑保持不变
	if m.pendingCount >= m.maxBatchTasks {
		m.ring.Submit(m.pendingCount)
		m.pendingCount = 0
	} else if m.pendingCount == n {
		time.AfterFunc(m.maxWaitDuration, m.ForceFlush)
	}
}

// 供定时器调用的强制刷新函数
func (m *UringManager) ForceFlush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingCount > 0 {
		m.ring.Submit(m.pendingCount)
		m.pendingCount = 0
	}
}
