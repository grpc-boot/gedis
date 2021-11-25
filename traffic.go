package gedis

import (
	"fmt"
	"time"
)

const (
	timeLimitKeyFormat = `%s:%s`

	tokenLimitScript = `
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
   `
)

func (p *pool) GetToken(key string, current int64, capacity, rate, reqNum, keyTimeoutSecond int) (ok bool, err error) {
	var (
		res int64
	)
	res, err = p.EvalOrSha4Int64(tokenLimitScript, 1, key, capacity, current, rate, reqNum, keyTimeoutSecond)
	return res == 1, err
}

func (p *pool) SecondLimitByToken(key string, limit int, reqNum int) (ok bool, err error) {
	var capacity int
	if limit < 8 {
		capacity = 2 * limit
	} else {
		capacity = int(1.25 * float32(limit))
	}

	return p.GetToken(key, time.Now().Unix(), capacity, limit, reqNum, 5)
}

func (p *pool) SecondLimitByTime(key string, limit int, reqNum int) (ok bool, err error) {
	key = fmt.Sprintf(timeLimitKeyFormat, key, time.Now().Format("150405"))

	newVal, err := p.IncrBy(key, reqNum)
	if err != nil {
		return false, err
	}

	if newVal == int64(reqNum) {
		_, _ = p.Expire(key, 10)
	}

	return newVal <= int64(limit), err
}

func (p *pool) MinuteLimitByTime(key string, limit int, reqNum int) (ok bool, err error) {
	key = fmt.Sprintf(timeLimitKeyFormat, key, time.Now().Format("1504"))

	newVal, err := p.IncrBy(key, reqNum)
	if err != nil {
		return false, err
	}

	if newVal == int64(reqNum) {
		_, _ = p.Expire(key, 600)
	}

	return newVal <= int64(limit), err
}

func (p *pool) HourLimitByTime(key string, limit int, reqNum int) (ok bool, err error) {
	key = fmt.Sprintf(timeLimitKeyFormat, key, time.Now().Format("15"))

	newVal, err := p.IncrBy(key, reqNum)
	if err != nil {
		return false, err
	}

	if newVal == int64(reqNum) {
		_, _ = p.Expire(key, 3680)
	}

	return newVal <= int64(limit), err
}

func (p *pool) DayLimitByTime(key string, limit int, reqNum int) (ok bool, err error) {
	key = fmt.Sprintf(timeLimitKeyFormat, key, time.Now().Format("20060102"))

	newVal, err := p.IncrBy(key, reqNum)
	if err != nil {
		return false, err
	}

	if newVal == int64(reqNum) {
		_, _ = p.Expire(key, 86400)
	}

	return newVal <= int64(limit), err
}
