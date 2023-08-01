package gedis

import (
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/grpc-boot/base"
	"github.com/grpc-boot/base/core/zaplogger"
)

const (
	Ok      = `OK`
	Success = 1
)

const (
	lockFormat = "ged_L:%s"
)

var (
	delLockScript = redigo.NewScript(1, `if redis.call('get', KEYS[1]) == ARGV[1]
            then
                return redis.call('del', KEYS[1])
            end
            return 0`)
)

type myPool struct {
	pool *redigo.Pool
	id   []byte
}

// NewPoolWithJson 实例化Pool
func NewPoolWithJson(jsonStr string) (p Pool, err error) {
	option := DefaultOption()
	err = base.JsonDecode(jsonStr, &option)
	if err != nil {
		return
	}

	return NewPool(option), err
}

// NewPool 实例化Pool
func NewPool(option Option) (p Pool) {
	var dialOptions = []redigo.DialOption{
		redigo.DialDatabase(int(option.Db)),
		redigo.DialConnectTimeout(time.Millisecond * time.Duration(option.ConnectTimeout)),
		redigo.DialReadTimeout(time.Millisecond * time.Duration(option.ReadTimeout)),
		redigo.DialWriteTimeout(time.Millisecond * time.Duration(option.WriteTimeout)),
	}

	if len(option.Auth) > 0 {
		dialOptions = append(dialOptions, redigo.DialPassword(option.Auth))
	}

	addr := fmt.Sprintf("%s:%d", option.Host, option.Port)
	id := fmt.Sprintf("%s:%d-%d", option.Host, option.Port, option.Index)

	pl := &redigo.Pool{
		MaxIdle:   option.MaxIdle,
		MaxActive: option.MaxActive,
		Wait:      option.Wait,
		Dial: func() (redigo.Conn, error) {
			return redigo.Dial("tcp", addr, dialOptions...)
		},
		TestOnBorrow: func(c redigo.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			if err != nil {
				Error("PING redis failed",
					zaplogger.Addr(id),
					zaplogger.Error(err),
				)
			}
			return err
		},
	}

	if option.MaxConnLifetimeSecond > 0 {
		pl.MaxConnLifetime = time.Second * time.Duration(option.MaxConnLifetimeSecond)
	}

	if option.IdleTimeoutSecond > 0 {
		pl.IdleTimeout = time.Second * time.Duration(option.IdleTimeoutSecond)
	}

	return &myPool{
		pool: pl,
		id:   []byte(id),
	}
}

func (mp *myPool) HashCode() uint32 {
	return crc32.ChecksumIEEE(mp.id)
}

func (mp *myPool) ActiveCount() (num int) {
	return mp.pool.ActiveCount()
}

func (mp *myPool) IdleCount() (num int) {
	return mp.pool.IdleCount()
}

func (mp *myPool) Stats() redigo.PoolStats {
	return mp.pool.Stats()
}

func (mp *myPool) Close() (err error) {
	return mp.pool.Close()
}

func (mp *myPool) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	start := time.Now()
	r := mp.pool.Get()
	defer r.Close()

	reply, err = r.Do(cmd, args...)

	if err != nil {
		Error("do redis cmd failed",
			zaplogger.Addr(base.Bytes2String(mp.id)),
			zaplogger.Error(err),
			zaplogger.Cmd(cmd),
			zaplogger.Args(args...),
			zaplogger.Duration(time.Since(start)),
		)
	}
	return
}

//region 1.0 Key

func (mp *myPool) Del(keys ...interface{}) (delNum int, err error) {
	return redigo.Int(mp.Do("DEL", keys...))
}

func (mp *myPool) Exists(key interface{}) (exists bool, err error) {
	var suc int
	suc, err = redigo.Int(mp.Do("EXISTS", key))
	return suc == Success, err
}

func (mp *myPool) Expire(key interface{}, second int64) (success int, err error) {
	return redigo.Int(mp.Do("EXPIRE", key, second))
}

func (mp *myPool) ExpireAt(key interface{}, unixTime int64) (success int, err error) {
	return redigo.Int(mp.Do("EXPIREAT", key, unixTime))
}

func (mp *myPool) Ttl(key interface{}) (second int64, err error) {
	return redigo.Int64(mp.Do("TTL", key))
}

func (mp *myPool) Persist(key interface{}) (success int, err error) {
	return redigo.Int(mp.Do("PERSIST", key))
}

func (mp *myPool) Scan(cursor int, match string, count int) (newCursor int, keys []string, err error) {
	var values []interface{}
	if match == "" {
		values, err = redigo.Values(mp.Do("SCAN", cursor))
	} else if count == 0 {
		values, err = redigo.Values(mp.Do("SCAN", cursor, "MATCH", match))
	} else {
		values, err = redigo.Values(mp.Do("SCAN", cursor, "MATCH", match, "COUNT", count))
	}

	if err != nil {
		return 0, nil, err
	}

	keys, err = redigo.Strings(values[1], err)
	if err != nil {
		return 0, nil, err
	}

	newCursor, _ = values[0].(int)
	return
}

func (mp *myPool) Keys(pattern string) (keys []string, err error) {
	return redigo.Strings(mp.Do("KEYS", pattern))
}

func (mp *myPool) Dump(key string) (serializedValue string, err error) {
	return String(mp.Do("DUMP", key))
}

func (mp *myPool) Restore(key string, pttl int64, serializedValue string) (ok bool, err error) {
	var res string
	res, err = redigo.String(mp.Do("RESTORE", key, pttl, serializedValue))
	return res == Ok, err
}

func (mp *myPool) Move(key string, db int) (success int, err error) {
	return redigo.Int(mp.Do("MOVE", key, db))
}

func (mp *myPool) RandomKey() (key string, err error) {
	return String(mp.Do("RANDOMKEY"))
}

func (mp *myPool) ReName(key string, newKey string) (ok bool, err error) {
	var res string
	res, err = redigo.String(mp.Do("RENAME", key, newKey))
	return res == Ok, err
}

func (mp *myPool) ReNameNx(key string, newKey string) (success int, err error) {
	return redigo.Int(mp.Do("RENAMENX", key, newKey))
}

func (mp *myPool) Type(key string) (t string, err error) {
	return redigo.String(mp.Do("TYPE", key))
}

//endregion

//region 1.1 String

func (mp *myPool) Append(key string, value interface{}) (strLength int, err error) {
	return redigo.Int(mp.Do("APPEND", key, value))
}

func (mp *myPool) StrLen(key string) (strLength int, err error) {
	return redigo.Int(mp.Do("STRLEN", key))
}

func (mp *myPool) Get(key string) (val string, err error) {
	return redigo.String(mp.Do("GET", key))
}

func (mp *myPool) GetBytes(key string) (val []byte, err error) {
	return redigo.Bytes(mp.Do("GET", key))
}

func (mp *myPool) MGet(keys ...string) (values []string, err error) {
	var (
		args = make([]interface{}, 0, len(keys))
	)

	for _, key := range keys {
		args = append(args, key)
	}

	return redigo.Strings(mp.Do("MGET", args...))
}

func (mp *myPool) MGetMap(keys ...string) (keyValue map[string]string, err error) {
	var (
		values []string
		args   = make([]interface{}, 0, len(keys))
	)
	for _, key := range keys {
		args = append(args, key)
	}
	values, err = redigo.Strings(mp.Do("MGET", args...))
	if err != nil {
		return nil, err
	}

	keyValue = make(map[string]string, len(values))
	for index, key := range keys {
		keyValue[key] = values[index]
	}
	return
}

func (mp *myPool) MGetBytesMap(keys ...string) (keyValue map[string][]byte, err error) {
	var (
		values [][]byte
		args   = make([]interface{}, 0, len(keys))
	)
	for _, key := range keys {
		args = append(args, key)
	}
	values, err = redigo.ByteSlices(mp.Do("MGET", args...))
	if err != nil {
		return nil, err
	}

	keyValue = make(map[string][]byte, len(values))
	for index, key := range keys {
		keyValue[key] = values[index]
	}
	return
}

func (mp *myPool) MSet(key1 string, value1 interface{}, keyValues ...interface{}) (ok bool, err error) {
	var (
		res  string
		args = make([]interface{}, 0, len(keyValues)+2)
	)

	args = append(args, key1, value1)
	args = append(args, keyValues...)

	res, err = redigo.String(mp.Do("MSET", args...))
	return res == Ok, err
}

func (mp *myPool) MSetByMap(keyValues map[string]interface{}) (ok bool, err error) {
	var (
		res  string
		args = make([]interface{}, 0, 2*len(keyValues))
	)

	for key, value := range keyValues {
		args = append(args, key, value)
	}

	res, err = redigo.String(mp.Do("MSET", args...))
	return res == Ok, err
}

func (mp *myPool) Set(key string, value interface{}, args ...interface{}) (ok bool, err error) {
	var (
		res    string
		params = make([]interface{}, len(args)+2)
	)

	params[0] = key
	params[1] = value
	for index, _ := range args {
		params[index+2] = args[index]
	}

	res, err = String(mp.Do("SET", params...))
	return res == Ok, err
}

func (mp *myPool) SetEx(key string, seconds int, value interface{}) (ok bool, err error) {
	var res string
	res, err = redigo.String(mp.Do("SETEX", key, seconds, value))
	return res == Ok, err
}

func (mp *myPool) SetNx(key string, value interface{}) (ok int, err error) {
	return redigo.Int(mp.Do("SETNX", key, value))
}

func (mp *myPool) GetSet(key string, value interface{}) (oldValue string, err error) {
	return String(mp.Do("GETSET", key, value))
}

func (mp *myPool) Incr(key string) (val int64, err error) {
	return redigo.Int64(mp.Do("INCR", key))
}

func (mp *myPool) Decr(key string) (val int64, err error) {
	return redigo.Int64(mp.Do("DECR", key))
}

func (mp *myPool) IncrBy(key string, increment int) (val int64, err error) {
	return redigo.Int64(mp.Do("INCRBY", key, increment))
}

func (mp *myPool) DecrBy(key string, decrement int) (val int64, err error) {
	return redigo.Int64(mp.Do("DECRBY", key, decrement))
}

func (mp *myPool) IncrByFloat(key string, increment float64) (val float64, err error) {
	return redigo.Float64(mp.Do("INCRBYFLOAT", key, increment))
}

func (mp *myPool) SetRange(key string, offset int, val string) (strLength int, err error) {
	return redigo.Int(mp.Do("SETRANGE", key, offset, val))
}

func (mp *myPool) GetRange(key string, start, end int) (val string, err error) {
	return String(mp.Do("GETRANGE", key, start, end))
}

func (mp *myPool) SetBit(key string, offset int, bit int8) (oldBit int, err error) {
	return redigo.Int(mp.Do("SETBIT", key, offset, bit))
}

func (mp *myPool) GetBit(key string, offset int) (bit int, err error) {
	return redigo.Int(mp.Do("GETBIT", key, offset))
}

func (mp *myPool) BitCount(key string, args ...interface{}) (num int, err error) {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, key)
	params = append(params, args...)
	return redigo.Int(mp.Do("BITCOUNT", params...))
}

//endregion

//region 1.2 Hash

func (mp *myPool) HSet(key string, field string, value interface{}) (isNew int, err error) {
	return redigo.Int(mp.Do("HSET", key, field, value))
}

func (mp *myPool) HSetNx(key string, field string, value interface{}) (ok int, err error) {
	return redigo.Int(mp.Do("HSETNX", key, field, value))
}

func (mp *myPool) HGet(key string, field string) (value string, err error) {
	return String(mp.Do("HGET", key, field))
}

func (mp *myPool) HMSet(key string, field1 string, value1 interface{}, args ...interface{}) (ok bool, err error) {
	var (
		res    string
		params = make([]interface{}, 0, len(args)+3)
	)

	params = append(params, key, field1, value1)
	params = append(params, args...)

	res, err = redigo.String(mp.Do("HMSET", params...))
	return res == Ok, err
}

func (mp *myPool) HMSetMap(key string, keyValues map[string]interface{}) (ok bool, err error) {
	var (
		res  string
		args = make([]interface{}, 0, 2*len(keyValues)+1)
	)

	args = append(args, key)
	for k, v := range keyValues {
		args = append(args, k, v)
	}

	res, err = redigo.String(mp.Do("HMSET", args...))
	return res == Ok, err
}

func (mp *myPool) HMGet(key string, fields ...string) (values []string, err error) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}
	return redigo.Strings(mp.Do("HMGET", args...))
}

func (mp *myPool) HMGetMap(key string, fields ...string) (keyValues map[string]string, err error) {
	var (
		values []string
		args   = make([]interface{}, 0, len(fields)+1)
	)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}
	values, err = redigo.Strings(mp.Do("HMGET", args...))
	if err != nil {
		return nil, err
	}

	keyValues = make(map[string]string, len(fields))
	for index, field := range fields {
		keyValues[field] = values[index]
	}

	return
}

func (mp *myPool) HGetAll(key string) (keyValues map[string]string, err error) {
	return redigo.StringMap(mp.Do("HGETALL", key))
}

func (mp *myPool) HGetAllBytes(key string) (keyValues map[string][]byte, err error) {
	return BytesMap(mp.Do("HGETALL", key))
}
func (mp *myPool) HDel(key string, fields ...string) (delNum int, err error) {
	var (
		args = make([]interface{}, 0, len(fields)+1)
	)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}
	return redigo.Int(mp.Do("HDEL", args...))
}

func (mp *myPool) HExists(key string, field string) (exists bool, err error) {
	var suc int
	suc, err = redigo.Int(mp.Do("HEXISTS", key, field))
	return suc == Success, err
}

func (mp *myPool) HIncrBy(key string, field string, increment int) (val int64, err error) {
	return redigo.Int64(mp.Do("HINCRBY", key, field, increment))
}

func (mp *myPool) HIncrByFloat(key string, field string, increment float64) (val float64, err error) {
	return redigo.Float64(mp.Do("HINCRBYFLOAT", key, field, increment))
}

func (mp *myPool) HKeys(key string) (fields []string, err error) {
	return redigo.Strings(mp.Do("HKEYS", key))
}

func (mp *myPool) HVals(key string) (values []string, err error) {
	return redigo.Strings(mp.Do("HVALS", key))
}

func (mp *myPool) HLen(key string) (length int, err error) {
	return redigo.Int(mp.Do("HLEN", key))
}

//endregion

//region 1.3 List

func (mp *myPool) LLen(key string) (listLength int, err error) {
	return redigo.Int(mp.Do("LLEN", key))
}

func (mp *myPool) LPush(key string, values ...interface{}) (listLength int, err error) {
	var (
		args = make([]interface{}, 0, len(values)+1)
	)
	args = append(args, key)
	for _, value := range values {
		args = append(args, value)
	}
	return redigo.Int(mp.Do("LPUSH", args...))
}

func (mp *myPool) LPushX(key string, value interface{}) (listLength int, err error) {
	return redigo.Int(mp.Do("LPUSHX", key, value))
}

func (mp *myPool) LPop(key string) (value string, err error) {
	return String(mp.Do("LPOP", key))
}

func (mp *myPool) LIndex(key string, index int) (value string, err error) {
	return String(mp.Do("LINDEX", key, index))
}

func (mp *myPool) LRange(key string, start, stop int) (values []string, err error) {
	return redigo.Strings(mp.Do("LRANGE", key, start, stop))
}

func (mp *myPool) LSet(key string, index int, value interface{}) (ok bool, err error) {
	var suc string
	suc, err = String(mp.Do("LSET", key, index, value))
	return suc == Ok, err
}

func (mp *myPool) LTrim(key string, start, stop int) (ok bool, err error) {
	var suc string
	suc, err = String(mp.Do("LTRIM", key, start, stop))
	return suc == Ok, err
}

func (mp *myPool) RPush(key string, values ...interface{}) (listLength int, err error) {
	var (
		args = make([]interface{}, 0, len(values)+1)
	)
	args = append(args, key)
	for _, value := range values {
		args = append(args, value)
	}
	return redigo.Int(mp.Do("RPUSH", args...))
}

func (mp *myPool) RPushX(key string, value interface{}) (listLength int, err error) {
	return redigo.Int(mp.Do("RPUSHX", key, value))
}

func (mp *myPool) RPop(key string) (value string, err error) {
	return String(mp.Do("RPOP", key))
}

//endregion

//region 1.4 Set

func (mp *myPool) SAdd(key string, members ...interface{}) (addNum int, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)
	args = append(args, key)
	args = append(args, members...)
	return redigo.Int(mp.Do("SADD", args...))
}

func (mp *myPool) SMembers(key string) (members []string, err error) {
	return redigo.Strings(mp.Do("SMEMBERS", key))
}

func (mp *myPool) SIsMember(key string, member interface{}) (exists bool, err error) {
	var suc int
	suc, err = redigo.Int(mp.Do("SISMEMBER", key, member))
	return suc == Success, err
}

func (mp *myPool) SCard(key string) (count int, err error) {
	return redigo.Int(mp.Do("SCARD", key))
}

func (mp *myPool) SPop(key string) (member string, err error) {
	return String(mp.Do("SPOP", key))
}

func (mp *myPool) SRandMember(key string, count int) (members []string, err error) {
	return redigo.Strings(mp.Do("SRANDMEMBER", key, count))
}

func (mp *myPool) SRem(key string, members ...interface{}) (removeNum int, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)
	args = append(args, key)
	args = append(args, members...)
	return redigo.Int(mp.Do("SREM", args...))
}

func (mp *myPool) SMove(sourceSetKey, destinationSetKey string, member interface{}) (success int, err error) {
	return redigo.Int(mp.Do("SMOVE", sourceSetKey, destinationSetKey, member))
}

func (mp *myPool) SDiff(keys ...interface{}) (members []string, err error) {
	return redigo.Strings(mp.Do("SDIFF", keys...))
}

func (mp *myPool) SDiffStore(destinationSetKey string, keys ...string) (memberCount int, err error) {
	var (
		args = make([]interface{}, 0, len(keys)+1)
	)
	args = append(args, destinationSetKey)
	for _, key := range keys {
		args = append(args, key)
	}
	return redigo.Int(mp.Do("SDIFFSTORE", args...))
}

func (mp *myPool) SInter(keys ...interface{}) (members []string, err error) {
	return redigo.Strings(mp.Do("SINTER", keys...))
}

func (mp *myPool) SInterStore(destinationSetKey string, keys ...string) (memberCount int, err error) {
	var (
		args = make([]interface{}, 0, len(keys)+1)
	)
	args = append(args, destinationSetKey)
	for _, key := range keys {
		args = append(args, key)
	}
	return redigo.Int(mp.Do("SINTERSTORE", args...))
}

func (mp *myPool) SUnion(keys ...interface{}) (members []string, err error) {
	return redigo.Strings(mp.Do("SUNION", keys...))
}

func (mp *myPool) SUnionStore(destinationSetKey string, keys ...string) (memberCount int, err error) {
	var (
		args = make([]interface{}, 0, len(keys)+1)
	)
	args = append(args, destinationSetKey)
	for _, key := range keys {
		args = append(args, key)
	}
	return redigo.Int(mp.Do("SUNIONSTORE", args...))
}

func (mp *myPool) SScan(key string, cursor int, match string, count int) (newCursor int, keys []string, err error) {
	var values []interface{}
	if match == "" {
		values, err = redigo.Values(mp.Do("SSCAN", key, cursor))
	} else if count == 0 {
		values, err = redigo.Values(mp.Do("SSCAN", key, cursor, "MATCH", match))
	} else {
		values, err = redigo.Values(mp.Do("SSCAN", key, cursor, "MATCH", match, "COUNT", count))
	}

	if err != nil {
		return 0, nil, err
	}

	keys, err = redigo.Strings(values[1], err)
	if err != nil {
		return 0, nil, err
	}

	newCursor, _ = values[0].(int)
	return
}

//endregion

//region 1.5 ZSet

func (mp *myPool) ZAdd(key string, score, value interface{}, scoreAndValues ...interface{}) (createNum int, err error) {
	var (
		args = make([]interface{}, 0, len(scoreAndValues)+3)
	)
	args = append(args, key)
	args = append(args, score, value)
	for _, p := range scoreAndValues {
		args = append(args, p)
	}
	return redigo.Int(mp.Do("ZADD", args...))
}

func (mp *myPool) ZAddMap(key string, membersMap map[string]interface{}) (createNum int, err error) {
	var (
		args = make([]interface{}, 0, 2*len(membersMap)+1)
	)
	args = append(args, key)
	for value, score := range membersMap {
		args = append(args, score, value)
	}
	return redigo.Int(mp.Do("ZADD", args...))
}

func (mp *myPool) ZCard(key string) (count int, err error) {
	return redigo.Int(mp.Do("ZCARD", key))
}

func (mp *myPool) ZCount(key string, minScore, maxScore interface{}) (count int, err error) {
	return redigo.Int(mp.Do("ZCOUNT", key, minScore, maxScore))
}

func (mp *myPool) ZIncrBy(key string, increment interface{}, member string) (newScore string, err error) {
	return String(mp.Do("ZINCRBY", key, increment, member))
}

func (mp *myPool) ZRange(key string, startIndex, stopIndex int) (members []string, err error) {
	return redigo.Strings(mp.Do("ZRANGE", key, startIndex, stopIndex))
}

func (mp *myPool) ZRevRange(key string, startIndex, stopIndex int) (members []string, err error) {
	return redigo.Strings(mp.Do("ZREVRANGE", key, startIndex, stopIndex))
}

func (mp *myPool) ZRangeWithScore(key string, startIndex, stopIndex int) (members map[string]string, err error) {
	return redigo.StringMap(mp.Do("ZRANGE", key, startIndex, stopIndex, "WITHSCORES"))
}

func (mp *myPool) ZRevRangeWithScore(key string, startIndex, stopIndex int) (members map[string]string, err error) {
	return redigo.StringMap(mp.Do("ZREVRANGE", key, startIndex, stopIndex, "WITHSCORES"))
}

func (mp *myPool) ZRangeByScore(key string, minScore, maxScore interface{}, offset, limit int) (members []string, err error) {
	if limit == 0 {
		return redigo.Strings(mp.Do("ZRANGEBYSCORE", key, minScore, maxScore))
	}
	return redigo.Strings(mp.Do("ZRANGEBYSCORE", key, minScore, maxScore, "LIMIT", offset, limit))
}

func (mp *myPool) ZRevRangeByScore(key string, maxScore, minScore interface{}, offset, limit int) (members []string, err error) {
	if limit == 0 {
		return redigo.Strings(mp.Do("ZREVRANGEBYSCORE", key, maxScore, minScore))
	}
	return redigo.Strings(mp.Do("ZREVRANGEBYSCORE", key, maxScore, minScore, "LIMIT", offset, limit))
}

func (mp *myPool) ZRangeByScoreWithScore(key string, minScore, maxScore interface{}, offset, limit int) (members map[string]string, err error) {
	if limit == 0 {
		return redigo.StringMap(mp.Do("ZRANGEBYSCORE", key, minScore, maxScore, "WITHSCORES"))
	}
	return redigo.StringMap(mp.Do("ZRANGEBYSCORE", key, minScore, maxScore, "WITHSCORES", "LIMIT", offset, limit))
}

func (mp *myPool) ZRevRangeByScoreWithScore(key string, maxScore, minScore interface{}, offset, limit int) (members map[string]string, err error) {
	if limit == 0 {
		return redigo.StringMap(mp.Do("ZREVRANGEBYSCORE", key, maxScore, minScore, "WITHSCORES"))
	}
	return redigo.StringMap(mp.Do("ZREVRANGEBYSCORE", key, maxScore, minScore, "WITHSCORES", "LIMIT", offset, limit))
}

func (mp *myPool) ZRank(key, member string) (rankIndex int, err error) {
	return redigo.Int(mp.Do("ZRANK", key, member))
}

func (mp *myPool) ZRevRank(key, member string) (rankIndex int, err error) {
	return redigo.Int(mp.Do("ZREVRANK", key, member))
}

func (mp *myPool) ZScore(key, member string) (score string, err error) {
	return String(mp.Do("ZSCORE", key, member))
}

func (mp *myPool) ZRem(key string, members ...interface{}) (removeNum int, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)
	args = append(args, key)
	args = append(args, members...)
	return redigo.Int(mp.Do("ZREM", args))
}

func (mp *myPool) ZRemRangeByRank(key string, startIndex, stopIndex int) (removeNum int, err error) {
	return redigo.Int(mp.Do("ZREMRANGEBYRANK", key, startIndex, stopIndex))
}

func (mp *myPool) ZRemRangeByScore(key string, minScore, maxScore interface{}) (removeNum int, err error) {
	return redigo.Int(mp.Do("ZREMRANGEBYSCORE", key, minScore, maxScore))
}

func (mp *myPool) ZScan(key string, cursor int, match string, count int) (newCursor int, keys []string, err error) {
	var values []interface{}
	if match == "" {
		values, err = redigo.Values(mp.Do("ZSCAN", key, cursor))
	} else if count == 0 {
		values, err = redigo.Values(mp.Do("ZSCAN", key, cursor, "MATCH", match))
	} else {
		values, err = redigo.Values(mp.Do("ZSCAN", key, cursor, "MATCH", match, "COUNT", count))
	}

	if err != nil {
		return 0, nil, err
	}

	keys, err = redigo.Strings(values[1], err)
	if err != nil {
		return 0, nil, err
	}

	newCursor, _ = values[0].(int)
	return
}

//endregion

//region 1.7 Pub/Sub

func (mp *myPool) Publish(channel string, msg string) (receiveNum int, err error) {
	return redigo.Int(mp.Do("PUBLISH", channel, msg))
}

func (mp *myPool) PubSubChannels(pattern string) (channels []string, err error) {
	return redigo.Strings(mp.Do("PUBSUB", "CHANNELS", pattern))
}

//endregion

//region 1.8 Script

func (mp *myPool) EvalOrSha(script *redigo.Script, keysAndArgs ...interface{}) (reply interface{}, err error) {
	r := mp.pool.Get()
	defer r.Close()

	return script.Do(r, keysAndArgs...)
}

func (mp *myPool) EvalOrSha4Int64(script *redigo.Script, keysAndArgs ...interface{}) (res int64, err error) {
	r := mp.pool.Get()
	defer r.Close()

	return redigo.Int64(script.Do(r, keysAndArgs...))
}

func (mp *myPool) EvalOrSha4String(script *redigo.Script, keysAndArgs ...interface{}) (res string, err error) {
	r := mp.pool.Get()
	defer r.Close()

	return String(script.Do(r, keysAndArgs...))
}

//endregion

//region 1.9 Transaction

func (mp *myPool) Exec(multi Multi) (values []interface{}, err error) {
	start := time.Now()
	r := mp.pool.Get()

	defer func() {
		ReleaseMulti(multi)
		_ = r.Close()
	}()

	if multi.Kind() == Transaction {
		_ = r.Send("MULTI")
	}

	for _, cmd := range multi.CmdList() {
		_ = r.Send(cmd.cmd, cmd.args...)
	}

	if multi.Kind() == Pipeline {
		values, err = redigo.Values(r.Do(""))
		if err != nil {
			Error("do redis pipeline failed",
				zaplogger.Addr(base.Bytes2String(mp.id)),
				zaplogger.Error(err),
				zaplogger.Duration(time.Since(start)),
			)
		}
		return
	}

	values, err = redigo.Values(r.Do("EXEC"))
	if err != nil {
		Error("do redis multi failed",
			zaplogger.Addr(base.Bytes2String(mp.id)),
			zaplogger.Error(err),
			zaplogger.Duration(time.Since(start)),
		)
	}

	return
}

//endregion

//region 1.10 Lock

func (mp *myPool) Acquire(key string, timeoutSecond int) (token int64, err error) {
	var (
		ok bool
	)

	token = time.Now().UnixNano()
	ok, err = mp.Set(fmt.Sprintf(lockFormat, key), token, "NX", "EX", timeoutSecond)
	if !ok {
		return 0, err
	}
	return token, nil
}

func (mp *myPool) Release(key string, token int64) (ok bool, err error) {
	var (
		suc int64
	)
	suc, err = mp.EvalOrSha4Int64(delLockScript, fmt.Sprintf(lockFormat, key), token)
	return suc == 1, err
}

//endregion

//region 2.0 Server

func (mp *myPool) ClientList() (clients []string, err error) {
	var clientsStr string
	clientsStr, err = String(mp.Do("CLIENT", "LIST"))
	if err != nil {
		return nil, err
	}

	return strings.Split(clientsStr, "\n"), nil
}

func (mp *myPool) ConfigGet(pattern string) (conf map[string]string, err error) {
	return redigo.StringMap(mp.Do("CONFIG", "GET", pattern))
}

func (mp *myPool) ConfigSet(param string, value interface{}) (ok bool, err error) {
	var res string
	res, err = redigo.String(mp.Do("CONFIG", "SET", param, value))
	return res == Ok, err
}

//endregion
