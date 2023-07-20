package gedis

import "sync/atomic"

type entry struct {
	createdAt    int64
	updatedAt    int64
	lastAccessAt int64
	flag         int64
	hitCount     uint64
	value        atomic.Value
}

func newEntry(current int64, value []byte) *entry {
	ent := &entry{
		createdAt:    current,
		updatedAt:    current,
		lastAccessAt: current,
	}

	ent.value.Store(value)

	return ent
}

func (e *entry) getValue() []byte {
	value, _ := e.value.Load().([]byte)
	return value
}

func (e *entry) getHitCount() uint64 {
	return atomic.LoadUint64(&e.hitCount)
}

func (e *entry) hitIncr() uint64 {
	return atomic.AddUint64(&e.hitCount, 1)
}

func (e *entry) hit(timeoutSecond, current int64) bool {
	return e.getUpdatedAt()+timeoutSecond > current
}

func (e *entry) getLastAccessAt() int64 {
	return atomic.LoadInt64(&e.lastAccessAt)
}

func (e *entry) access(current int64) uint64 {
	atomic.StoreInt64(&e.lastAccessAt, current)
	return e.hitIncr()
}

func (e *entry) update(current int64, value []byte) {
	atomic.StoreInt64(&e.updatedAt, current)
	e.value.Store(value)
}

func (e *entry) getUpdatedAt() int64 {
	return atomic.LoadInt64(&e.updatedAt)
}

func (e *entry) getCreatedAt() int64 {
	return atomic.LoadInt64(&e.createdAt)
}

func (e *entry) lock(current, timeout int64) bool {
	flag := atomic.LoadInt64(&e.flag)
	if current-flag < timeout {
		return false
	}

	return atomic.CompareAndSwapInt64(&e.flag, flag, current)
}

func (e *entry) unlock(current int64) {
	atomic.CompareAndSwapInt64(&e.flag, current, 0)
}
