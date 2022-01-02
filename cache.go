package gedis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	redigo "github.com/garyburd/redigo/redis"
)

const (
	overflowFlag      = `increment or decrement would overflow`
	cacheKeyFormat    = `ged_C:%s`
	lockTimeoutSecond = 5
)

// Item 缓存Item
type Item struct {
	Hit       bool   `json:"hit"`
	CreateAt  int64  `json:"create_at"`
	UpdateAt  int64  `json:"update_at"`
	ExpireAt  int64  `json:"expire_at"`
	MissCount int64  `json:"miss_count"`
	HitCount  int64  `json:"hit_count"`
	Value     []byte `json:"value"`
}

func (p *pool) updateCache(key string, item *Item, current int64, timeoutSecond int64, handler func() []byte) (err error) {
	if item.CreateAt < 1 {
		item.CreateAt = current
	}
	item.UpdateAt = current
	item.ExpireAt = current + timeoutSecond
	item.Value = handler()

	if item.Value != nil {
		m := PipeMulti()
		m.HMSet(key, "expire_at", item.ExpireAt, "update_at", current, "create_at", item.CreateAt, "value", item.Value)
		m.HIncrBy(key, "miss_count", 1)
		var res []interface{}
		res, err = p.Exec(m)
		if len(res) == 2 {
			switch mc := res[1].(type) {
			case int64:
				item.MissCount = mc
			case redigo.Error:
				if strings.Contains(mc.Error(), overflowFlag) {
					_, err = p.HSet(key, "miss_count", 0)
				}
			}
		}
	}

	return
}

// CacheRemove 统计设置缓存过期的方式移除缓存
func (p *pool) CacheRemove(key string) (ok bool, err error) {
	key = fmt.Sprintf(cacheKeyFormat, key)
	_, err = p.HSet(key, "expire_at", 0)
	return err == nil, err
}

// CacheGet 通用缓存
func (p *pool) CacheGet(key string, timeoutSecond int64, handler func() []byte) (item Item, err error) {
	var (
		redisValue map[string]string
		current    = time.Now().Unix()
	)

	key = fmt.Sprintf(cacheKeyFormat, key)

	redisValue, err = p.HGetAll(key)
	if err != nil {
		return
	}

	//redis中没有数据
	if redisValue == nil {
		err = p.updateCache(key, &item, current, timeoutSecond, handler)
		return item, err
	}

	//从redis中取值
	if createAt, exists := redisValue["create_at"]; exists {
		item.CreateAt, _ = strconv.ParseInt(createAt, 10, 64)
	}
	if missCount, exists := redisValue["miss_count"]; exists {
		item.MissCount, _ = strconv.ParseInt(missCount, 10, 64)
	}
	if hitCount, exists := redisValue["hit_count"]; exists {
		item.HitCount, _ = strconv.ParseInt(hitCount, 10, 64)
	}
	if expireAt, exists := redisValue["expire_at"]; exists {
		item.ExpireAt, _ = strconv.ParseInt(expireAt, 10, 64)
	}

	val, ok := redisValue["value"]
	if item.ExpireAt == 0 || !ok {
		err = p.updateCache(key, &item, current, timeoutSecond, handler)
		return item, err
	}

	item.Value = []byte(val)

	//缓存有效
	if item.ExpireAt > current {
		item.Hit = true
		item.HitCount, err = p.HIncrBy(key, "hit_count", 1)
		if err != nil && strings.Contains(err.Error(), overflowFlag) {
			_, err = p.HSet(key, "hit_count", 0)
		}
		return item, err
	}

	//-------------------缓存失效-----------------------
	//去拿锁
	token, _ := p.Acquire(key, lockTimeoutSecond)

	//未获得锁
	if token == 0 {
		return item, nil
	}

	defer func() {
		_, err = p.Release(key, token)
	}()

	err = p.updateCache(key, &item, current, timeoutSecond, handler)
	return item, err
}
