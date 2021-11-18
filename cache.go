package gedis

import (
	"strconv"
	"time"
)

const (
	maxTimeoutSecond  = 3600 * 24 * 14
	lockTimeoutSecond = 5
)

type Item struct {
	ExpireAt int64  `json:"expire_at"`
	Value    []byte `json:"value"`
}

func (r *redis) updateCache(key string, item *Item, current int64, timeoutSecond int64, handler func() []byte) []byte {
	item.ExpireAt = current + timeoutSecond
	item.Value = handler()

	if item.Value != nil {
		m, _ := r.Multi(Pipeline)
		m.HMSet(key, "expire_at", item.ExpireAt, "value", item.Value)
		m.Expire(key, maxTimeoutSecond)
		_, _ = r.Exec(m)
	}
	return item.Value
}

func (r *redis) Cache(key string, timeoutSecond int64, handler func() []byte) (value []byte) {
	var (
		item       Item
		redisValue map[string]string
		err        error
		current    = time.Now().Unix()
	)

	redisValue, err = r.HGetAll(key)
	if err != nil {
		return
	}

	//redis中没有数据
	if redisValue == nil {
		return r.updateCache(key, &item, current, timeoutSecond, handler)
	}

	//从redis中取值
	expireAt, exists := redisValue["expire_at"]
	if !exists {
		return r.updateCache(key, &item, current, timeoutSecond, handler)
	}
	val, ok := redisValue["value"]
	if !ok {
		return r.updateCache(key, &item, current, timeoutSecond, handler)
	}
	item.Value = []byte(val)

	item.ExpireAt, _ = strconv.ParseInt(expireAt, 10, 64)
	//缓存有效
	if item.ExpireAt > current {
		return item.Value
	}

	//-------------------缓存失效-----------------------
	//去拿锁
	token, _ := r.Acquire(key, lockTimeoutSecond)
	//未获得锁
	if token == 0 {
		return item.Value
	}

	//获得锁
	defer func() {
		_, _ = r.Release(key, token)
	}()

	return r.updateCache(key, &item, current, timeoutSecond, handler)
}
