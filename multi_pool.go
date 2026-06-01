//go:build linux

package goiouring

import (
	"sync"
	"unsafe"
)

type MultiPool struct {
	// pools 存放空闲的 buffer 索引
	pools [17]chan *[]byte
	// inflight 充当“生命支持系统”，Key 是 uintptr(Data指针)，Value 是 *[]byte
	// 只要 buffer 在这里，GC 就绝对不会回收它
	inflight sync.Map
}

func NewMultiPool() *MultiPool {
	mp := &MultiPool{}
	for i := 0; i < len(mp.pools); i++ {
		// 每个 size class 预分配一定数量的初始 buffer，避免冷启动延迟
		// 这里的容量可以根据业务压力调整
		mp.pools[i] = make(chan *[]byte, 1024)
	}
	return mp
}

// Get 获取一个 Buffer。如果池子空了，会 New 一个。
func (mp *MultiPool) Get(size int) (bufPtr *[]byte) {
	if size <= 0 {
		size = 1
	}
	idx := 0
	for (1 << idx) < size {
		idx++
	}

	if idx >= len(mp.pools) {
		// 超过池子最大范围，直接分配
		b := make([]byte, size)
		bufPtr = &b
	} else {
		select {
		case bufPtr = <-mp.pools[idx]:
			// 拿到现成的，恢复其长度
			*bufPtr = (*bufPtr)[:cap(*bufPtr)]
		default:
			// 池子空了，New 一个新的
			b := make([]byte, 1<<idx)
			bufPtr = &b
		}
	}

	// 【关键】无论是从池子拿的还是 New 的，都要在 inflight 登记保活
	// 拿到该切片底层数组的首地址作为 Key
	ptr := uintptr(unsafe.Pointer(&(*bufPtr)[0]))
	mp.inflight.Store(ptr, bufPtr)

	return bufPtr
}

// Put 归还到池子。注意：现在不需要传入 poolIdx，我们可以自动推算。
func (mp *MultiPool) Put(bufPtr *[]byte) {
	if bufPtr == nil || len(*bufPtr) == 0 {
		return
	}

	// 1. 获取物理地址
	ptr := uintptr(unsafe.Pointer(&(*bufPtr)[0]))

	// 2. 从 inflight 移除（解除保活）
	// 移除后，如果没有人引用它，且池子也满了，它才会被 GC 回收
	mp.inflight.Delete(ptr)

	// 3. 计算它属于哪个 size class
	capacity := cap(*bufPtr)
	idx := 0
	for (1 << idx) < capacity {
		idx++
	}

	if idx >= len(mp.pools) {
		return // 超过范围的大对象，直接丢弃让 GC 回收
	}

	// 4. 尝试归还池子
	select {
	case mp.pools[idx] <- bufPtr:
		// 成功归还
	default:
		// 池子满了，直接丢弃即可。因为已经执行了 inflight.Delete，GC 会处理它
	}
}
