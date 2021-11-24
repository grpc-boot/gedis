package gedis

import (
	"time"
)

const (
	tokenLimitScript = `
		local tKey        = KEYS[1]
		local tCapacity   = tonumber(ARGV[1])
		local current     = tonumber(ARGV[2])
		local rate        = tonumber(ARGV[3])
		local reqNum      = tonumber(ARGV[4])
		local tKeyTimeout = 600
		if #ARGV > 4 then
			tKeyTimeout = tonumber(ARGV[5])
		end
		
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

func (p *pool) LimitWithSecond(key string, limit int, reqNum int) (ok bool, err error) {
	var (
		res int64
	)

	res, err = p.EvalOrSha4Int64(tokenLimitScript, 1, key, limit, time.Now().Unix(), limit, reqNum)
	return res == 1, err
}
