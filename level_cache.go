package gedis

import (
	"fmt"
	"sync"

	"github.com/grpc-boot/base"
)

var DefaultLocalCache sync.Map

func (mp *myPool) LevelCache(localCache *sync.Map, key string, current, timeoutSecond int64, handler Handler) (value []byte, err error) {
	key = fmt.Sprintf(cacheKeyFormat, key)

	var (
		localValue, _ = localCache.Load(key)
		ent           *entry
		ok            bool
	)

	ent, ok = localValue.(*entry)
	if ok {
		// 本地缓存命中或者获得锁失败
		if ent.hit(timeoutSecond, current) || !ent.lock(current, lockTimeoutSecond) {
			ent.access(current)
			return ent.getValue(), nil
		}
		defer ent.unlock(current)
	}

	var redisValue map[string][]byte
	redisValue, err = mp.HGetAllBytes(key)
	if err != nil {
		if ent != nil {
			return ent.getValue(), nil
		}
		return
	}

	item := Item{}
	//redis中没有数据
	if redisValue == nil {
		err = mp.updateCache(key, &item, current, handler)
		if err == nil {
			// 更新本地缓存
			if ent == nil {
				localCache.Store(key, newEntry(current, item.Value))
			} else {
				ent.update(current, item.Value)
			}
		} else if ent != nil {
			return ent.getValue(), nil
		}

		return item.Value, err
	}

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

	//-------------------未获取到redis数据-----------------------
	if item.UpdatedAt == 0 || !ok {
		err = mp.updateCache(key, &item, current, handler)
		if err == nil {
			// 更新本地缓存
			if ent == nil {
				localCache.Store(key, newEntry(current, item.Value))
			} else {
				ent.update(current, item.Value)
			}
		} else if ent != nil {
			return ent.getValue(), nil
		}

		return item.Value, err
	}

	//-------------------缓存有效-----------------------
	if item.Hit(timeoutSecond, current) {
		// 更新本地缓存
		if ent == nil {
			localCache.Store(key, newEntry(current, item.Value))
		} else {
			ent.update(current, item.Value)
		}
		return item.Value, err
	}

	//-------------------缓存失效-----------------------
	//加锁
	token, _ := mp.Acquire(key, lockTimeoutSecond)
	//未获得锁
	if token == 0 {
		return item.Value, nil
	}

	// 获得锁
	err = mp.updateCache(key, &item, current, handler)
	if err == nil {
		// 更新本地缓存
		if ent == nil {
			localCache.Store(key, newEntry(current, item.Value))
		} else {
			ent.update(current, item.Value)
		}
		_, _ = mp.Release(key, token)
	}

	return item.Value, err
}
