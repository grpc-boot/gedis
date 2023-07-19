package gedis

import "sync"

const (
	Transaction = 1
	Pipeline    = 2
)

var (
	multiPool = sync.Pool{
		New: func() interface{} {
			return &multi{
				cmdList: make([]Cmd, 0),
			}
		},
	}

	PipeMulti = func() Multi {
		m := multiPool.Get().(*multi)
		m.kind = Pipeline
		return m
	}

	TransMulti = func() Multi {
		m := multiPool.Get().(*multi)
		m.kind = Transaction
		return m
	}

	ReleaseMulti = func(m Multi) {
		m.Reset()
		multiPool.Put(m)
	}
)

type Multi interface {
	//-----------------Key--------------------------
	Del(keys ...interface{}) Multi
	Exists(key interface{}) Multi
	Expire(key interface{}, second int64) Multi
	ExpireAt(key interface{}, unixTime int64) Multi
	Ttl(key interface{}) Multi
	Persist(key interface{}) Multi
	Keys(pattern string) Multi
	Dump(key string) Multi
	Restore(key string, pttl int64, serializedValue string) Multi
	Move(key string, db int) Multi
	RandomKey() Multi
	ReName(key string, newKey string) Multi
	ReNameNx(key string, newKey string) Multi
	Type(key string) Multi

	//-----------------String--------------------------
	Append(key string, value interface{}) Multi
	Get(key string) Multi
	MGet(keys ...string) Multi
	MSet(key1 string, value1 interface{}, keyValues ...interface{}) Multi
	MSetByMap(keyValues map[string]interface{}) Multi
	Set(key string, value interface{}, args ...interface{}) Multi
	SetEx(key string, seconds int, value interface{}) Multi
	SetNx(key string, value interface{}) Multi
	GetSet(key string, value interface{}) Multi
	Incr(key string) Multi
	Decr(key string) Multi
	IncrBy(key string, increment int) Multi
	DecrBy(key string, decrement int) Multi
	IncrByFloat(key string, increment float64) Multi
	SetRange(key string, offset int, val string) Multi
	GetRange(key string, start, end int) Multi
	SetBit(key string, offset int, bit int8) Multi
	GetBit(key string, offset int) Multi
	BitCount(key string, args ...interface{}) Multi

	//-----------------Hash--------------------------
	HSet(key string, field string, value interface{}) Multi
	HSetNx(key string, field string, value interface{}) Multi
	HGet(key string, field string) Multi
	HMSet(key string, field1 string, value1 interface{}, args ...interface{}) Multi
	HMSetMap(key string, keyValues map[string]interface{}) Multi
	HMGet(key string, fields ...string) Multi
	HGetAll(key string) Multi
	HDel(key string, fields ...string) Multi
	HExists(key string, field string) Multi
	HIncrBy(key string, field string, increment int) Multi
	HIncrByFloat(key string, field string, increment float64) Multi
	HKeys(key string) Multi
	HVals(key string) Multi
	HLen(key string) Multi

	//-----------------List--------------------------
	LLen(key string) Multi
	LPush(key string, values ...interface{}) Multi
	LPushX(key string, value interface{}) Multi
	LPop(key string) Multi
	LIndex(key string, index int) Multi
	LRange(key string, start, stop int) Multi
	LSet(key string, index int, value interface{}) Multi
	LTrim(key string, start, stop int) Multi
	RPush(key string, values ...interface{}) Multi
	RPushX(key string, value interface{}) Multi
	RPop(key string) Multi

	//--------------------Set---------------------------
	SAdd(key string, members ...interface{}) Multi
	SMembers(key string) Multi
	SIsMember(key string, member interface{}) Multi
	SCard(key string) Multi
	SPop(key string) Multi
	SRandMember(key string, count int) Multi
	SRem(key string, members ...interface{}) Multi
	SMove(sourceSetKey, destinationSetKey string, member interface{}) Multi
	SDiff(keys ...interface{}) Multi
	SDiffStore(destinationSetKey string, keys ...string) Multi
	SInter(keys ...interface{}) Multi
	SInterStore(destinationSetKey string, keys ...string) Multi
	SUnion(keys ...interface{}) Multi
	SUnionStore(destinationSetKey string, keys ...string) Multi

	//--------------------ZSet---------------------------
	ZAdd(key string, score, value interface{}, scoreAndValues ...interface{}) Multi
	ZAddMap(key string, membersMap map[string]interface{}) Multi
	ZCard(key string) Multi
	ZCount(key string, minScore, maxScore interface{}) Multi
	ZIncrBy(key string, increment interface{}, member string) Multi
	ZRange(key string, startIndex, stopIndex int) Multi
	ZRevRange(key string, startIndex, stopIndex int) Multi
	ZRangeWithScore(key string, startIndex, stopIndex int) Multi
	ZRevRangeWithScore(key string, startIndex, stopIndex int) Multi
	ZRangeByScore(key string, minScore, maxScore interface{}, offset, limit int) Multi
	ZRevRangeByScore(key string, maxScore, minScore interface{}, offset, limit int) Multi
	ZRangeByScoreWithScore(key string, minScore, maxScore interface{}, offset, limit int) Multi
	ZRevRangeByScoreWithScore(key string, maxScore, minScore interface{}, offset, limit int) Multi
	ZRank(key, member string) Multi
	ZRevRank(key, member string) Multi
	ZScore(key, member string) Multi
	ZRem(key string, members ...interface{}) Multi
	ZRemRangeByRank(key string, startIndex, stopIndex int) Multi

	//--------------------ZSet---------------------------
	GeoAdd(key string, longitude, latitude float64, member interface{}, args ...interface{}) Multi
	GeoHash(key string, members ...interface{}) Multi
	GeoDel(key string, members ...interface{}) Multi
	GeoDist(key string, member1, member2 interface{}, unit string) Multi
	GeoPos(key string, members ...interface{}) Multi
	GeoRadius(key string, longitude, latitude float64, radius interface{}, unit string, count int, sort string) Multi
	GeoRadiusByMember(key string, member interface{}, radius interface{}, unit string, count int, sort string) Multi

	Reset()
	Kind() uint8
	CmdList() []Cmd
}

type Cmd struct {
	cmd  string
	args []interface{}
}

type multi struct {
	kind    uint8
	cmdList []Cmd
}

func (m *multi) Del(keys ...interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "DEL", args: keys})
	return m
}

func (m *multi) Exists(key interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "EXISTS", args: []interface{}{key}})
	return m
}

func (m *multi) Expire(key interface{}, second int64) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "EXPIRE", args: []interface{}{key, second}})
	return m
}

func (m *multi) ExpireAt(key interface{}, unixTime int64) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "EXPIREAT", args: []interface{}{key, unixTime}})
	return m
}

func (m *multi) Ttl(key interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "TTL", args: []interface{}{key}})
	return m
}

func (m *multi) Persist(key interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "PERSIST", args: []interface{}{key}})
	return m
}

func (m *multi) Scan(cursor int, match string, count int) Multi {
	cmd := Cmd{
		cmd: "SCAN",
	}

	if match == "" {
		cmd.args = []interface{}{cursor}
	} else if count == 0 {
		cmd.args = []interface{}{cursor, "MATCH", match}
	} else {
		cmd.args = []interface{}{cursor, "MATCH", match, "COUNT", count}
	}

	m.cmdList = append(m.cmdList, cmd)
	return m
}

func (m *multi) Keys(pattern string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "KEYS", args: []interface{}{pattern}})
	return m
}

func (m *multi) Dump(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "DUMP", args: []interface{}{key}})
	return m
}

func (m *multi) Restore(key string, pttl int64, serializedValue string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "RESTORE", args: []interface{}{key, pttl, serializedValue}})
	return m
}

func (m *multi) Move(key string, db int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "MOVE", args: []interface{}{key, db}})
	return m
}

func (m *multi) RandomKey() Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "RANDOMKEY", args: make([]interface{}, 0, 0)})
	return m
}

func (m *multi) ReName(key string, newKey string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "RENAME", args: []interface{}{key, newKey}})
	return m
}

func (m *multi) ReNameNx(key string, newKey string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "RENAMENX", args: []interface{}{key, newKey}})
	return m
}

func (m *multi) Type(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "TYPE", args: []interface{}{key}})
	return m
}

func (m *multi) Append(key string, value interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "APPEND", args: []interface{}{key, value}})
	return m
}

func (m *multi) StrLen(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "STRLEN", args: []interface{}{key}})
	return m
}

func (m *multi) Get(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "GET", args: []interface{}{key}})
	return m
}

func (m *multi) MGet(keys ...string) Multi {
	var (
		args = make([]interface{}, len(keys))
	)

	for index, _ := range keys {
		args[index] = keys[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "MGET", args: args})
	return m
}

func (m *multi) MSet(key1 string, value1 interface{}, keyValues ...interface{}) Multi {
	var (
		args = make([]interface{}, len(keyValues)+2)
	)

	args[0] = key1
	args[1] = value1
	for index, _ := range keyValues {
		args[index+2] = keyValues[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "MSET", args: args})
	return m
}

func (m *multi) MSetByMap(keyValues map[string]interface{}) Multi {
	var (
		args = make([]interface{}, 2*len(keyValues))
	)

	start := 0
	for key, value := range keyValues {
		args[start] = key
		args[start+1] = value
		start += 2
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "MSET", args: args})
	return m
}

func (m *multi) Set(key string, value interface{}, args ...interface{}) Multi {
	var (
		params = make([]interface{}, len(args)+2)
	)

	params[0] = key
	params[1] = value
	for index, _ := range args {
		params[index+2] = args[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "SET", args: params})
	return m
}

func (m *multi) SetEx(key string, seconds int, value interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SETEX", args: []interface{}{key, seconds, value}})
	return m
}

func (m *multi) SetNx(key string, value interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SETNX", args: []interface{}{key, value}})
	return m
}

func (m *multi) GetSet(key string, value interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "GETSET", args: []interface{}{key, value}})
	return m
}

func (m *multi) Incr(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "INCR", args: []interface{}{key}})
	return m
}

func (m *multi) Decr(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "DECR", args: []interface{}{key}})
	return m
}

func (m *multi) IncrBy(key string, increment int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "INCRBY", args: []interface{}{key, increment}})
	return m
}

func (m *multi) DecrBy(key string, decrement int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "DECRBY", args: []interface{}{key, decrement}})
	return m
}

func (m *multi) IncrByFloat(key string, increment float64) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "INCRBYFLOAT", args: []interface{}{key, increment}})
	return m
}

func (m *multi) SetRange(key string, offset int, val string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SETRANGE", args: []interface{}{key, offset, val}})
	return m
}

func (m *multi) GetRange(key string, start, end int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "GETRANGE", args: []interface{}{key, start, end}})
	return m
}

func (m *multi) SetBit(key string, offset int, bit int8) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SETBIT", args: []interface{}{key, offset, bit}})
	return m
}

func (m *multi) GetBit(key string, offset int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "GETBIT", args: []interface{}{key, offset}})
	return m
}

func (m *multi) BitCount(key string, args ...interface{}) Multi {
	params := make([]interface{}, len(args)+1)
	params[0] = key
	for index, _ := range args {
		params[index+1] = args[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "BITCOUNT", args: params})
	return m
}

func (m *multi) HSet(key string, field string, value interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "HSET", args: []interface{}{key, field, value}})
	return m
}

func (m *multi) HSetNx(key string, field string, value interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "HSETNX", args: []interface{}{key, field, value}})
	return m
}

func (m *multi) HGet(key string, field string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "HGET", args: []interface{}{key, field}})
	return m
}

func (m *multi) HMSet(key string, field1 string, value1 interface{}, args ...interface{}) Multi {
	var (
		params = make([]interface{}, len(args)+3)
	)

	params[0] = key
	params[1] = field1
	params[2] = value1

	for index, _ := range args {
		params[index+3] = args[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "HMSET", args: params})
	return m
}

func (m *multi) HMSetMap(key string, keyValues map[string]interface{}) Multi {
	var (
		args = make([]interface{}, 2*len(keyValues)+1)
	)

	args[0] = key
	index := 0
	for k, v := range keyValues {
		args[index+1] = k
		args[index+2] = v
		index += 2
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "HMSET", args: args})
	return m
}

func (m *multi) HMGet(key string, fields ...string) Multi {
	args := make([]interface{}, len(fields)+1)

	args[0] = key
	for index, _ := range fields {
		args[index+1] = fields[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "HMGET", args: args})
	return m
}

func (m *multi) HGetAll(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "HGETALL", args: []interface{}{key}})
	return m
}

func (m *multi) HDel(key string, fields ...string) Multi {
	var (
		args = make([]interface{}, len(fields)+1)
	)

	args[0] = key
	for index, _ := range fields {
		args[index+1] = fields[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "HDEL", args: args})
	return m
}

func (m *multi) HExists(key string, field string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "HEXISTS", args: []interface{}{key, field}})
	return m
}

func (m *multi) HIncrBy(key string, field string, increment int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "HINCRBY", args: []interface{}{key, field, increment}})
	return m
}

func (m *multi) HIncrByFloat(key string, field string, increment float64) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "HINCRBYFLOAT", args: []interface{}{key, field, increment}})
	return m
}

func (m *multi) HKeys(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "HKEYS", args: []interface{}{key}})
	return m
}

func (m *multi) HVals(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "HVALS", args: []interface{}{key}})
	return m
}

func (m *multi) HLen(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "HLEN", args: []interface{}{key}})
	return m
}

func (m *multi) LLen(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "LLEN", args: []interface{}{key}})
	return m
}

func (m *multi) LPush(key string, values ...interface{}) Multi {
	var (
		args = make([]interface{}, len(values)+1)
	)

	args[0] = key
	for index, _ := range values {
		args[index+1] = values[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "LPUSH", args: args})
	return m
}

func (m *multi) LPushX(key string, value interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "LPUSHX", args: []interface{}{key, value}})
	return m
}

func (m *multi) LPop(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "LPOP", args: []interface{}{key}})
	return m
}

func (m *multi) LIndex(key string, index int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "LINDEX", args: []interface{}{key, index}})
	return m
}

func (m *multi) LRange(key string, start, stop int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "LRANGE", args: []interface{}{key, start, stop}})
	return m
}

func (m *multi) LSet(key string, index int, value interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "LSET", args: []interface{}{key, index, value}})
	return m
}

func (m *multi) LTrim(key string, start, stop int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "LTRIM", args: []interface{}{key, start, stop}})
	return m
}

func (m *multi) RPush(key string, values ...interface{}) Multi {
	var (
		args = make([]interface{}, len(values)+1)
	)

	args[0] = key
	for index, _ := range values {
		args[index+1] = values[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "RPUSH", args: args})
	return m
}

func (m *multi) RPushX(key string, value interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "RPUSHX", args: []interface{}{key, value}})
	return m
}

func (m *multi) RPop(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "RPOP", args: []interface{}{key}})
	return m
}

func (m *multi) SAdd(key string, members ...interface{}) Multi {
	var (
		args = make([]interface{}, len(members)+1)
	)

	args[0] = key
	for index, _ := range members {
		args[index+1] = members[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "SADD", args: args})
	return m
}

func (m *multi) SMembers(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SMEMBERS", args: []interface{}{key}})
	return m
}

func (m *multi) SIsMember(key string, member interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SISMEMBER", args: []interface{}{key, member}})
	return m
}

func (m *multi) SCard(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SCARD", args: []interface{}{key}})
	return m
}

func (m *multi) SPop(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SPOP", args: []interface{}{key}})
	return m
}

func (m *multi) SRandMember(key string, count int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SRANDMEMBER", args: []interface{}{key, count}})
	return m
}

func (m *multi) SRem(key string, members ...interface{}) Multi {
	var (
		args = make([]interface{}, len(members)+1)
	)

	args[0] = key
	for index, _ := range members {
		args[index+1] = members[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "SREM", args: args})
	return m
}

func (m *multi) SMove(sourceSetKey, destinationSetKey string, member interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SMOVE", args: []interface{}{sourceSetKey, destinationSetKey, member}})
	return m
}

func (m *multi) SDiff(keys ...interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SDIFF", args: keys})
	return m
}

func (m *multi) SDiffStore(destinationSetKey string, keys ...string) Multi {
	var (
		args = make([]interface{}, len(keys)+1)
	)
	args[0] = destinationSetKey
	for index, _ := range keys {
		args[index+1] = keys[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "SDIFFSTORE", args: args})
	return m
}

func (m *multi) SInter(keys ...interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SINTER", args: keys})
	return m
}

func (m *multi) SInterStore(destinationSetKey string, keys ...string) Multi {
	var (
		args = make([]interface{}, len(keys)+1)
	)

	args[0] = destinationSetKey
	for index, _ := range keys {
		args[index+1] = keys[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "SINTERSTORE", args: args})
	return m
}

func (m *multi) SUnion(keys ...interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "SUNION", args: keys})
	return m
}

func (m *multi) SUnionStore(destinationSetKey string, keys ...string) Multi {
	var (
		args = make([]interface{}, len(keys)+1)
	)

	args[0] = destinationSetKey
	for index, _ := range keys {
		args[index+1] = keys[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "SUNIONSTORE", args: args})
	return m
}

func (m *multi) ZAdd(key string, score, value interface{}, scoreAndValues ...interface{}) Multi {
	var (
		args = make([]interface{}, len(scoreAndValues)+3)
	)

	args[0] = key
	args[1] = score
	args[2] = value

	for index, _ := range scoreAndValues {
		args[index+3] = scoreAndValues[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "ZADD", args: args})
	return m
}

func (m *multi) ZAddMap(key string, membersMap map[string]interface{}) Multi {
	var (
		args = make([]interface{}, 2*len(membersMap)+1)
	)

	args[0] = key
	start := 0
	for value, score := range membersMap {
		args[start+1] = score
		args[start+2] = value
		start += 2
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "ZADD", args: args})
	return m
}

func (m *multi) ZCard(key string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZCARD", args: []interface{}{key}})
	return m
}

func (m *multi) ZCount(key string, minScore, maxScore interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZCOUNT", args: []interface{}{key, minScore, maxScore}})
	return m
}

func (m *multi) ZIncrBy(key string, increment interface{}, member string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZINCRBY", args: []interface{}{key, increment, member}})
	return m
}

func (m *multi) ZRange(key string, startIndex, stopIndex int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZRANGE", args: []interface{}{key, startIndex, stopIndex}})
	return m
}

func (m *multi) ZRevRange(key string, startIndex, stopIndex int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZREVRANGE", args: []interface{}{key, startIndex, stopIndex}})
	return m
}

func (m *multi) ZRangeWithScore(key string, startIndex, stopIndex int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZRANGE", args: []interface{}{key, startIndex, stopIndex, "WITHSCORES"}})
	return m
}

func (m *multi) ZRevRangeWithScore(key string, startIndex, stopIndex int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZREVRANGE", args: []interface{}{key, startIndex, stopIndex, "WITHSCORES"}})
	return m
}

func (m *multi) ZRangeByScore(key string, minScore, maxScore interface{}, offset, limit int) Multi {
	if limit == 0 {
		m.cmdList = append(m.cmdList, Cmd{cmd: "ZRANGEBYSCORE", args: []interface{}{key, minScore, maxScore}})
	} else {
		m.cmdList = append(m.cmdList, Cmd{cmd: "ZRANGEBYSCORE", args: []interface{}{key, minScore, maxScore, "LIMIT", offset, limit}})
	}
	return m
}

func (m *multi) ZRevRangeByScore(key string, maxScore, minScore interface{}, offset, limit int) Multi {
	if limit == 0 {
		m.cmdList = append(m.cmdList, Cmd{cmd: "ZREVRANGEBYSCORE", args: []interface{}{key, maxScore, minScore}})
	} else {
		m.cmdList = append(m.cmdList, Cmd{cmd: "ZREVRANGEBYSCORE", args: []interface{}{key, maxScore, minScore, "LIMIT", offset, limit}})
	}
	return m
}

func (m *multi) ZRangeByScoreWithScore(key string, minScore, maxScore interface{}, offset, limit int) Multi {
	if limit == 0 {
		m.cmdList = append(m.cmdList, Cmd{cmd: "ZRANGEBYSCORE", args: []interface{}{key, minScore, maxScore, "WITHSCORES"}})
	} else {
		m.cmdList = append(m.cmdList, Cmd{cmd: "ZRANGEBYSCORE", args: []interface{}{key, minScore, maxScore, "WITHSCORES", "LIMIT", offset, limit}})
	}
	return m
}

func (m *multi) ZRevRangeByScoreWithScore(key string, maxScore, minScore interface{}, offset, limit int) Multi {
	if limit == 0 {
		m.cmdList = append(m.cmdList, Cmd{cmd: "ZREVRANGEBYSCORE", args: []interface{}{key, maxScore, minScore, "WITHSCORES"}})
	} else {
		m.cmdList = append(m.cmdList, Cmd{cmd: "ZREVRANGEBYSCORE", args: []interface{}{key, maxScore, minScore, "WITHSCORES", "LIMIT", offset, limit}})
	}
	return m
}

func (m *multi) ZRank(key, member string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZRANK", args: []interface{}{key, member}})
	return m
}

func (m *multi) ZRevRank(key, member string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZREVRANK", args: []interface{}{key, member}})
	return m
}

func (m *multi) ZScore(key, member string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZSCORE", args: []interface{}{key, member}})
	return m
}

func (m *multi) ZRem(key string, members ...interface{}) Multi {
	var (
		args = make([]interface{}, len(members)+1)
	)

	args[0] = key
	for index, _ := range members {
		args[index+1] = members[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "ZREM", args: args})
	return m
}

func (m *multi) ZRemRangeByRank(key string, startIndex, stopIndex int) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZREMRANGEBYRANK", args: []interface{}{key, startIndex, stopIndex}})
	return m
}

func (m *multi) ZRemRangeByScore(key string, minScore, maxScore interface{}) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "ZREMRANGEBYSCORE", args: []interface{}{key, minScore, maxScore}})
	return m
}

func (m *multi) Reset() {
	m.kind = 0
	m.cmdList = m.cmdList[:0]
}

func (m *multi) Kind() uint8 {
	return m.kind
}

func (m *multi) CmdList() []Cmd {
	return m.cmdList
}
