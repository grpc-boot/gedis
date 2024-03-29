package gedis

import (
	"fmt"
	"time"

	redigo "github.com/garyburd/redigo/redis"
)

const (
	timeLimitKeyFormat = `%s:%s`
)

var (
	tokenLimitScript = redigo.NewScript(1, `
		local tKey        = KEYS[1]
		local tCapacity   = tonumber(ARGV[1])
		local current     = tonumber(ARGV[2])
		local rate        = tonumber(ARGV[3])
		local reqNum      = tonumber(ARGV[4])
		local tKeyTimeout = tonumber(ARGV[5])
		
		local bucketInfo = redis.call('HMGET', tKey, 'last_add_time', 'remain_token_num')
		if not bucketInfo[1] then
			redis.call('HMSET', tKey, 'last_add_time', current, 'remain_token_num', tCapacity - 1)
			redis.call('EXPIRE', tKey, tKeyTimeout)
			return 1
		end
		
		local lastAddTime    = tonumber(bucketInfo[1])
		local remainTokenNum = tonumber(bucketInfo[2])
		
		local addTokenNum    = (current - lastAddTime) * rate
		if addTokenNum > 0 then
			lastAddTime    = current
			remainTokenNum = math.min(addTokenNum + remainTokenNum, tCapacity)
			redis.call('HSET', tKey, 'last_add_time', current)
		end
		
		if reqNum > remainTokenNum then
			return 0
		end
		
		redis.call('HSET', tKey, 'remain_token_num', remainTokenNum - reqNum)
		redis.call('EXPIRE', tKey, tKeyTimeout)
		return 1
   `)
)

func (mp *myPool) GetToken(key string, current int64, capacity, rate, reqNum, keyTimeoutSecond int) (ok bool, err error) {
	var (
		res int64
	)
	res, err = mp.EvalOrSha4Int64(tokenLimitScript, key, capacity, current, rate, reqNum, keyTimeoutSecond)
	return res == 1, err
}

func (mp *myPool) SecondLimitByToken(key string, limit int, reqNum int) (ok bool, err error) {
	var capacity int
	if limit < 8 {
		capacity = 2 * limit
	} else {
		capacity = int(1.25 * float32(limit))
	}

	return mp.GetToken(key, time.Now().Unix(), capacity, limit, reqNum, 5)
}

func (mp *myPool) SecondLimitByTime(key string, limit int, reqNum int) (ok bool, err error) {
	key = fmt.Sprintf(timeLimitKeyFormat, key, time.Now().Format("150405"))

	newVal, err := mp.IncrBy(key, reqNum)
	if err != nil {
		return false, err
	}

	if newVal == int64(reqNum) {
		_, _ = mp.Expire(key, 10)
	}

	return newVal <= int64(limit), err
}

func (mp *myPool) MinuteLimitByTime(key string, limit int, reqNum int) (ok bool, err error) {
	key = fmt.Sprintf(timeLimitKeyFormat, key, time.Now().Format("1504"))

	newVal, err := mp.IncrBy(key, reqNum)
	if err != nil {
		return false, err
	}

	if newVal == int64(reqNum) {
		_, _ = mp.Expire(key, 600)
	}

	return newVal <= int64(limit), err
}

func (mp *myPool) HourLimitByTime(key string, limit int, reqNum int) (ok bool, err error) {
	key = fmt.Sprintf(timeLimitKeyFormat, key, time.Now().Format("15"))

	newVal, err := mp.IncrBy(key, reqNum)
	if err != nil {
		return false, err
	}

	if newVal == int64(reqNum) {
		_, _ = mp.Expire(key, 3680)
	}

	return newVal <= int64(limit), err
}

func (mp *myPool) DayLimitByTime(key string, limit int, reqNum int) (ok bool, err error) {
	key = fmt.Sprintf(timeLimitKeyFormat, key, time.Now().Format("20060102"))

	newVal, err := mp.IncrBy(key, reqNum)
	if err != nil {
		return false, err
	}

	if newVal == int64(reqNum) {
		_, _ = mp.Expire(key, 86400)
	}

	return newVal <= int64(limit), err
}
