package gedis

import (
	"fmt"
	"strings"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/grpc-boot/base"
	"github.com/grpc-boot/base/core/zaplogger"
)

const (
	overflowFlag       = `increment or decrement would overflow`
	cacheKeyFormat     = `ged_C:%s`
	lockTimeoutSecond  = 8
	cacheTimeoutSecond = 3600 * 24 * 7
)

type Handler func() (value []byte, err error)

// Item 缓存Item
type Item struct {
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
	UpdatedCount int64  `json:"updated_count"`
	Value        []byte `json:"value"`
}

func (i *Item) Hit(timeoutSecond int64, current int64) bool {
	return i.UpdatedAt+timeoutSecond > current
}

func (mp *myPool) updateCache(key string, item *Item, current int64, handler Handler) (err error) {
	value, err := handler()
	if err != nil {
		Error("cache exec handler failed",
			zaplogger.Key(key),
			zaplogger.Error(err),
		)
		return
	}

	item.UpdatedAt = current
	item.Value = value
	if item.CreatedAt < 1 {
		item.CreatedAt = current
	}

	if item.Value != nil {
		m := PipeMulti()
		m.HMSet(key, "value", item.Value, "created_at", item.CreatedAt, "updated_at", current)
		m.HIncrBy(key, "updated_count", 1)
		m.Expire(key, cacheTimeoutSecond)

		var res []interface{}
		res, err = mp.Exec(m)
		if len(res) > 2 {
			switch uc := res[1].(type) {
			case int64:
				item.UpdatedCount = uc
			case redigo.Error:
				if strings.Contains(uc.Error(), overflowFlag) {
					_, err = mp.HSet(key, "updated_count", 0)
				}
			}
		}
	}

	return
}

// CacheRemove 设置缓存过期的方式移除缓存
func (mp *myPool) CacheRemove(key string) (ok bool, err error) {
	key = fmt.Sprintf(cacheKeyFormat, key)
	_, err = mp.HSet(key, "updated_at", 0)
	return err == nil, err
}

// CacheGet 通用缓存
func (mp *myPool) CacheGet(key string, current, timeoutSecond int64, handler Handler) (value []byte, err error) {
	item, err := mp.CacheGetItem(key, current, timeoutSecond, handler)
	return item.Value, err
}

// CacheGetItem 通用缓存
func (mp *myPool) CacheGetItem(key string, current, timeoutSecond int64, handler Handler) (item Item, err error) {
	var redisValue map[string][]byte

	key = fmt.Sprintf(cacheKeyFormat, key)
	redisValue, err = mp.HGetAllBytes(key)
	if err != nil {
		return
	}

	//redis中没有数据
	if redisValue == nil {
		err = mp.updateCache(key, &item, current, handler)
		return item, err
	}

	//从redis中取值
	val, ok := redisValue["value"]
	//从redis中取值
	if ok {
		createdAt, _ := redisValue["created_at"]
		updatedAt, _ := redisValue["updated_at"]
		updatedCount, _ := redisValue["updated_count"]

		item.Value = val
		item.UpdatedAt = base.Bytes2Int64(updatedAt)
		item.CreatedAt = base.Bytes2Int64(createdAt)
		item.UpdatedCount = base.Bytes2Int64(updatedCount)
	}

	if item.UpdatedAt == 0 || !ok {
		err = mp.updateCache(key, &item, current, handler)
		return item, err
	}

	item.Value = val

	//缓存有效
	if item.Hit(timeoutSecond, current) {
		return item, err
	}

	//-------------------缓存失效-----------------------
	//去拿锁
	token, _ := mp.Acquire(key, lockTimeoutSecond)
	//未获得锁
	if token == 0 {
		return item, nil
	}

	// 获得锁
	err = mp.updateCache(key, &item, current, handler)
	if err == nil {
		_, _ = mp.Release(key, token)
	}

	return item, err
}
