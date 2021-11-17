package gedis

import (
	"errors"
	redigo "github.com/garyburd/redigo/redis"
	"strings"
)

const (
	Ok      = `OK`
	Success = 1
)

var (
	ErrInvalidMultiKind = errors.New(`invalid multi kind`)
)

type Conn interface {
	Close() (err error)
	Do(cmd string, args ...interface{}) (reply interface{}, err error)

	//-----------------Key--------------------------
	Del(keys ...interface{}) (delNum int, err error)
	Exists(key interface{}) (exists bool, err error)
	Expire(key interface{}, second int64) (success int, err error)
	ExpireAt(key interface{}, unixTime int64) (success int, err error)
	Ttl(key interface{}) (second int64, err error)
	Persist(key interface{}) (success int, err error)
	Scan(cursor int, match string, count int) (newCursor int, keys []string, err error)
	Keys(pattern string) (keys []string, err error)
	Dump(key string) (serializedValue string, err error)
	Restore(key string, pttl int64, serializedValue string) (ok bool, err error)
	Move(key string, db int) (success int, err error)
	RandomKey() (key string, err error)
	ReName(key string, newKey string) (ok bool, err error)
	ReNameNx(key string, newKey string) (success int, err error)
	Type(key string) (t string, err error)

	//-----------------String--------------------------
	Append(key string, value interface{}) (strLength int, err error)
	Get(key string) (val string, err error)
	MGet(keys ...string) (values []string, err error)
	MGetMap(keys ...string) (keyValue map[string]string, err error)
	MSet(key1 string, value1 interface{}, keyValues ...interface{}) (ok bool, err error)
	MSetByMap(keyValues map[string]interface{}) (ok bool, err error)
	Set(key string, value interface{}, args ...interface{}) (ok bool, err error)
	SetEx(key string, seconds int, value interface{}) (ok bool, err error)
	SetNx(key string, value interface{}) (ok int, err error)
	GetSet(key string, value interface{}) (oldValue string, err error)
	Incr(key string) (val int64, err error)
	Decr(key string) (val int64, err error)
	IncrBy(key string, increment int) (val int64, err error)
	DecrBy(key string, decrement int) (val int64, err error)
	IncrByFloat(key string, increment float64) (val float64, err error)
	SetRange(key string, offset int, val string) (strLength int, err error)
	GetRange(key string, start, end int) (val string, err error)
	SetBit(key string, offset int, bit int8) (oldBit int, err error)
	GetBit(key string, offset int) (bit int, err error)
	BitCount(key string, args ...interface{}) (num int, err error)

	//-----------------Hash--------------------------
	HSet(key string, field string, value interface{}) (isNew int, err error)
	HSetNx(key string, field string, value interface{}) (ok int, err error)
	HGet(key string, field string) (value string, err error)
	HMSet(key string, field1 string, value1 interface{}, args ...interface{}) (ok bool, err error)
	HMSetMap(key string, keyValues map[string]interface{}) (ok bool, err error)
	HMGet(key string, fields ...string) (values []string, err error)
	HMGetMap(key string, fields ...string) (keyValues map[string]string, err error)
	HGetAll(key string) (keyValues map[string]string, err error)
	HDel(key string, fields ...string) (delNum int, err error)
	HExists(key string, field string) (exists bool, err error)
	HIncrBy(key string, field string, increment int) (val int64, err error)
	HIncrByFloat(key string, field string, increment float64) (val float64, err error)
	HKeys(key string) (fields []string, err error)
	HVals(key string) (values []string, err error)
	HLen(key string) (length int, err error)

	//-----------------List--------------------------
	LLen(key string) (listLength int, err error)
	LPush(key string, values ...interface{}) (listLength int, err error)
	LPushX(key string, value interface{}) (listLength int, err error)
	LPop(key string) (value string, err error)
	LIndex(key string, index int) (value string, err error)
	LRange(key string, start, stop int) (values []string, err error)
	LSet(key string, index int, value interface{}) (ok bool, err error)
	LTrim(key string, start, stop int) (ok bool, err error)
	RPush(key string, values ...interface{}) (listLength int, err error)
	RPushX(key string, value interface{}) (listLength int, err error)
	RPop(key string) (value string, err error)

	//--------------------Set---------------------------
	SAdd(key string, members ...interface{}) (addNum int, err error)
	SMembers(key string) (members []string, err error)
	SIsMember(key string, member interface{}) (exists bool, err error)
	SCard(key string) (count int, err error)
	SPop(key string) (member string, err error)
	SRandMember(key string, count int) (members []string, err error)
	SRem(key string, members ...interface{}) (removeNum int, err error)
	SMove(sourceSetKey, destinationSetKey string, member interface{}) (success int, err error)
	SDiff(keys ...interface{}) (members []string, err error)
	SDiffStore(destinationSetKey string, keys ...string) (memberCount int, err error)
	SInter(keys ...interface{}) (members []string, err error)
	SInterStore(destinationSetKey string, keys ...string) (memberCount int, err error)
	SUnion(keys ...interface{}) (members []string, err error)
	SUnionStore(destinationSetKey string, keys ...string) (memberCount int, err error)
	SScan(key string, cursor int, match string, count int) (newCursor int, keys []string, err error)

	//--------------------ZSet---------------------------
	ZAdd(key string, score, value interface{}, scoreAndValues ...interface{}) (createNum int, err error)
	ZAddMap(key string, membersMap map[string]interface{}) (createNum int, err error)
	ZCard(key string) (count int, err error)
	ZCount(key string, minScore, maxScore interface{}) (count int, err error)
	ZIncrBy(key string, increment interface{}, member string) (newScore string, err error)
	ZRange(key string, startIndex, stopIndex int) (members []string, err error)
	ZRevRange(key string, startIndex, stopIndex int) (members []string, err error)
	ZRangeWithScore(key string, startIndex, stopIndex int) (members map[string]string, err error)
	ZRevRangeWithScore(key string, startIndex, stopIndex int) (members map[string]string, err error)
	ZRangeByScore(key string, minScore, maxScore interface{}, offset, limit int) (members []string, err error)
	ZRevRangeByScore(key string, maxScore, minScore interface{}, offset, limit int) (members []string, err error)
	ZRangeByScoreWithScore(key string, minScore, maxScore interface{}, offset, limit int) (members map[string]string, err error)
	ZRevRangeByScoreWithScore(key string, maxScore, minScore interface{}, offset, limit int) (members map[string]string, err error)
	ZRank(key, member string) (rankIndex int, err error)
	ZRevRank(key, member string) (rankIndex int, err error)
	ZScore(key, member string) (score string, err error)
	ZRem(key string, members ...interface{}) (removeNum int, err error)
	ZRemRangeByRank(key string, startIndex, stopIndex int) (removeNum int, err error)
	ZScan(key string, cursor int, match string, count int) (newCursor int, keys []string, err error)

	//--------------------Pub/Sub---------------------------
	Publish(channel string, msg string) (receiveNum int, err error)
	PubSubChannels(pattern string) (channels []string, err error)

	//--------------------Pub/Sub---------------------------
	EvalOrSha(script string, keyCount int, keysAndArgs ...interface{}) (reply interface{}, err error)
	EvalOrSha4Int64(script string, keyCount int, keysAndArgs ...interface{}) (res int64, err error)
	EvalOrSha4String(script string, keyCount int, keysAndArgs ...interface{}) (res string, err error)

	//--------------------Transaction---------------------------
	Multi(kind uint8) (multi Multi, err error)
	Exec(multi Multi) (values []interface{}, err error)
	Watch(keys ...interface{}) (ok bool, err error)
	UnWatch(keys ...interface{}) (ok bool, err error)

	//-----------------Server--------------------------
	ClientList() (clients []string, err error)
	ConfigGet(pattern string) (conf map[string]string, err error)
	ConfigSet(param string, value interface{}) (ok bool, err error)
}

func newConn(conn redigo.Conn) Conn {
	return &redis{conn: conn}
}

type redis struct {
	conn redigo.Conn
}

func (r *redis) Close() (err error) {
	return r.conn.Close()
}

func (r *redis) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	return r.conn.Do(cmd, args...)
}

//region 1.0 Key

func (r *redis) Del(keys ...interface{}) (delNum int, err error) {
	return redigo.Int(r.conn.Do("DEL", keys...))
}

func (r *redis) Exists(key interface{}) (exists bool, err error) {
	var suc int
	suc, err = redigo.Int(r.conn.Do("EXISTS", key))
	return suc == Success, err
}

func (r *redis) Expire(key interface{}, second int64) (success int, err error) {
	return redigo.Int(r.conn.Do("EXPIRE", key, second))
}

func (r *redis) ExpireAt(key interface{}, unixTime int64) (success int, err error) {
	return redigo.Int(r.conn.Do("EXPIREAT", key, unixTime))
}

func (r *redis) Ttl(key interface{}) (second int64, err error) {
	return redigo.Int64(r.conn.Do("TTL", key))
}

func (r *redis) Persist(key interface{}) (success int, err error) {
	return redigo.Int(r.conn.Do("PERSIST", key))
}

func (r *redis) Scan(cursor int, match string, count int) (newCursor int, keys []string, err error) {
	var values []interface{}
	if match == "" {
		values, err = redigo.Values(r.conn.Do("SCAN", cursor))
	} else if count == 0 {
		values, err = redigo.Values(r.conn.Do("SCAN", cursor, "MATCH", match))
	} else {
		values, err = redigo.Values(r.conn.Do("SCAN", cursor, "MATCH", match, "COUNT", count))
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

func (r *redis) Keys(pattern string) (keys []string, err error) {
	return redigo.Strings(r.conn.Do("KEYS", pattern))
}

func (r *redis) Dump(key string) (serializedValue string, err error) {
	return String(r.conn.Do("DUMP", key))
}

func (r *redis) Restore(key string, pttl int64, serializedValue string) (ok bool, err error) {
	var res string
	res, err = redigo.String(r.conn.Do("RESTORE", key, pttl, serializedValue))
	return res == Ok, err
}

func (r *redis) Move(key string, db int) (success int, err error) {
	return redigo.Int(r.conn.Do("MOVE", key, db))
}

func (r *redis) RandomKey() (key string, err error) {
	return String(r.conn.Do("RANDOMKEY"))
}

func (r *redis) ReName(key string, newKey string) (ok bool, err error) {
	var res string
	res, err = redigo.String(r.conn.Do("RENAME", key, newKey))
	return res == Ok, err
}

func (r *redis) ReNameNx(key string, newKey string) (success int, err error) {
	return redigo.Int(r.conn.Do("RENAMENX", key, newKey))
}

func (r *redis) Type(key string) (t string, err error) {
	return redigo.String(r.conn.Do("TYPE", key))
}

//endregion

//region 1.1 String

func (r *redis) Append(key string, value interface{}) (strLength int, err error) {
	return redigo.Int(r.conn.Do("APPEND", key, value))
}

func (r *redis) StrLen(key string) (strLength int, err error) {
	return redigo.Int(r.conn.Do("STRLEN", key))
}

func (r *redis) Get(key string) (val string, err error) {
	return redigo.String(r.conn.Do("GET", key))
}

func (r *redis) MGet(keys ...string) (values []string, err error) {
	var (
		args = make([]interface{}, 0, len(keys))
	)

	for _, key := range keys {
		args = append(args, key)
	}

	return redigo.Strings(r.conn.Do("MGET", args...))
}

func (r *redis) MGetMap(keys ...string) (keyValue map[string]string, err error) {
	var (
		values []string
		args   = make([]interface{}, 0, len(keys))
	)
	for _, key := range keys {
		args = append(args, key)
	}
	values, err = redigo.Strings(r.conn.Do("MGET", args...))
	if err != nil {
		return nil, err
	}

	keyValue = make(map[string]string, len(values))
	for index, key := range keys {
		keyValue[key] = values[index]
	}
	return
}

func (r *redis) MSet(key1 string, value1 interface{}, keyValues ...interface{}) (ok bool, err error) {
	var (
		res  string
		args = make([]interface{}, 0, len(keyValues)+2)
	)

	args = append(args, key1, value1)
	args = append(args, keyValues...)

	res, err = redigo.String(r.conn.Do("MSET", args...))
	return res == Ok, err
}

func (r *redis) MSetByMap(keyValues map[string]interface{}) (ok bool, err error) {
	var (
		res  string
		args = make([]interface{}, 0, 2*len(keyValues))
	)

	for key, value := range keyValues {
		args = append(args, key, value)
	}

	res, err = redigo.String(r.conn.Do("MSET", args...))
	return res == Ok, err
}

func (r *redis) Set(key string, value interface{}, args ...interface{}) (ok bool, err error) {
	var (
		res    string
		params = make([]interface{}, 0, len(args)+2)
	)
	params = append(params, key, value)
	params = append(params, args...)
	res, err = redigo.String(r.conn.Do("SET", params...))
	return res == Ok, err
}

func (r *redis) SetEx(key string, seconds int, value interface{}) (ok bool, err error) {
	var res string
	res, err = redigo.String(r.conn.Do("SETEX", key, seconds, value))
	return res == Ok, err
}

func (r *redis) SetNx(key string, value interface{}) (ok int, err error) {
	return redigo.Int(r.conn.Do("SETNX", key, value))
}

func (r *redis) GetSet(key string, value interface{}) (oldValue string, err error) {
	return String(r.conn.Do("GETSET", key, value))
}

func (r *redis) Incr(key string) (val int64, err error) {
	return redigo.Int64(r.conn.Do("INCR", key))
}

func (r *redis) Decr(key string) (val int64, err error) {
	return redigo.Int64(r.conn.Do("DECR", key))
}

func (r *redis) IncrBy(key string, increment int) (val int64, err error) {
	return redigo.Int64(r.conn.Do("INCRBY", key, increment))
}

func (r *redis) DecrBy(key string, decrement int) (val int64, err error) {
	return redigo.Int64(r.conn.Do("DECRBY", key, decrement))
}

func (r *redis) IncrByFloat(key string, increment float64) (val float64, err error) {
	return redigo.Float64(r.conn.Do("INCRBYFLOAT", key, increment))
}

func (r *redis) SetRange(key string, offset int, val string) (strLength int, err error) {
	return redigo.Int(r.conn.Do("SETRANGE", key, offset, val))
}

func (r *redis) GetRange(key string, start, end int) (val string, err error) {
	return String(r.conn.Do("GETRANGE", key, start, end))
}

func (r *redis) SetBit(key string, offset int, bit int8) (oldBit int, err error) {
	return redigo.Int(r.conn.Do("SETBIT", key, offset, bit))
}

func (r *redis) GetBit(key string, offset int) (bit int, err error) {
	return redigo.Int(r.conn.Do("GETBIT", key, offset))
}

func (r *redis) BitCount(key string, args ...interface{}) (num int, err error) {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, key)
	params = append(params, args...)
	return redigo.Int(r.conn.Do("BITCOUNT", params...))
}

//endregion

//region 1.2 Hash

func (r *redis) HSet(key string, field string, value interface{}) (isNew int, err error) {
	return redigo.Int(r.conn.Do("HSET", key, field, value))
}

func (r *redis) HSetNx(key string, field string, value interface{}) (ok int, err error) {
	return redigo.Int(r.conn.Do("HSETNX", key, field, value))
}

func (r *redis) HGet(key string, field string) (value string, err error) {
	return String(r.conn.Do("HGET", key, field))
}

func (r *redis) HMSet(key string, field1 string, value1 interface{}, args ...interface{}) (ok bool, err error) {
	var (
		res    string
		params = make([]interface{}, 0, len(args)+3)
	)

	params = append(params, key, field1, value1)
	params = append(params, args...)

	res, err = redigo.String(r.conn.Do("HMSET", params...))
	return res == Ok, err
}

func (r *redis) HMSetMap(key string, keyValues map[string]interface{}) (ok bool, err error) {
	var (
		res  string
		args = make([]interface{}, 0, 2*len(keyValues)+1)
	)

	args = append(args, key)
	for k, v := range keyValues {
		args = append(args, k, v)
	}

	res, err = redigo.String(r.conn.Do("HMSET", args...))
	return res == Ok, err
}

func (r *redis) HMGet(key string, fields ...string) (values []string, err error) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}
	return redigo.Strings(r.conn.Do("HMGET", args...))
}

func (r *redis) HMGetMap(key string, fields ...string) (keyValues map[string]string, err error) {
	var (
		values []string
		args   = make([]interface{}, 0, len(fields)+1)
	)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}
	values, err = redigo.Strings(r.conn.Do("HMGET", args...))
	if err != nil {
		return nil, err
	}

	keyValues = make(map[string]string, len(fields))
	for index, field := range fields {
		keyValues[field] = values[index]
	}

	return
}

func (r *redis) HGetAll(key string) (keyValues map[string]string, err error) {
	return redigo.StringMap(r.conn.Do("HGETALL", key))
}

func (r *redis) HDel(key string, fields ...string) (delNum int, err error) {
	var (
		args = make([]interface{}, 0, len(fields)+1)
	)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}
	return redigo.Int(r.conn.Do("HDEL", args...))
}

func (r *redis) HExists(key string, field string) (exists bool, err error) {
	var suc int
	suc, err = redigo.Int(r.conn.Do("HEXISTS", key, field))
	return suc == Success, err
}

func (r *redis) HIncrBy(key string, field string, increment int) (val int64, err error) {
	return redigo.Int64(r.conn.Do("HINCRBY", key, field, increment))
}

func (r *redis) HIncrByFloat(key string, field string, increment float64) (val float64, err error) {
	return redigo.Float64(r.conn.Do("HINCRBYFLOAT", key, field, increment))
}

func (r *redis) HKeys(key string) (fields []string, err error) {
	return redigo.Strings(r.conn.Do("HKEYS", key))
}

func (r *redis) HVals(key string) (values []string, err error) {
	return redigo.Strings(r.conn.Do("HVALS", key))
}

func (r *redis) HLen(key string) (length int, err error) {
	return redigo.Int(r.conn.Do("HLEN", key))
}

//endregion

//region 1.3 List

func (r *redis) LLen(key string) (listLength int, err error) {
	return redigo.Int(r.conn.Do("LLEN", key))
}

func (r *redis) LPush(key string, values ...interface{}) (listLength int, err error) {
	var (
		args = make([]interface{}, 0, len(values)+1)
	)
	args = append(args, key)
	for _, value := range values {
		args = append(args, value)
	}
	return redigo.Int(r.conn.Do("LPUSH", args...))
}

func (r *redis) LPushX(key string, value interface{}) (listLength int, err error) {
	return redigo.Int(r.conn.Do("LPUSHX", key, value))
}

func (r *redis) LPop(key string) (value string, err error) {
	return String(r.conn.Do("LPOP", key))
}

func (r *redis) LIndex(key string, index int) (value string, err error) {
	return String(r.conn.Do("LINDEX", key, index))
}

func (r *redis) LRange(key string, start, stop int) (values []string, err error) {
	return redigo.Strings(r.conn.Do("LRANGE", key, start, stop))
}

func (r *redis) LSet(key string, index int, value interface{}) (ok bool, err error) {
	var suc string
	suc, err = String(r.conn.Do("LSET", key, index, value))
	return suc == Ok, err
}

func (r *redis) LTrim(key string, start, stop int) (ok bool, err error) {
	var suc string
	suc, err = String(r.conn.Do("LTRIM", key, start, stop))
	return suc == Ok, err
}

func (r *redis) RPush(key string, values ...interface{}) (listLength int, err error) {
	var (
		args = make([]interface{}, 0, len(values)+1)
	)
	args = append(args, key)
	for _, value := range values {
		args = append(args, value)
	}
	return redigo.Int(r.conn.Do("RPUSH", args...))
}

func (r *redis) RPushX(key string, value interface{}) (listLength int, err error) {
	return redigo.Int(r.conn.Do("RPUSHX", key, value))
}

func (r *redis) RPop(key string) (value string, err error) {
	return String(r.conn.Do("RPOP", key))
}

//endregion

//region 1.4 Set

func (r *redis) SAdd(key string, members ...interface{}) (addNum int, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)
	args = append(args, key)
	args = append(args, members...)
	return redigo.Int(r.conn.Do("SADD", args...))
}

func (r *redis) SMembers(key string) (members []string, err error) {
	return redigo.Strings(r.conn.Do("SMEMBERS", key))
}

func (r *redis) SIsMember(key string, member interface{}) (exists bool, err error) {
	var suc int
	suc, err = redigo.Int(r.conn.Do("SISMEMBER", key, member))
	return suc == Success, err
}

func (r *redis) SCard(key string) (count int, err error) {
	return redigo.Int(r.conn.Do("SCARD", key))
}

func (r *redis) SPop(key string) (member string, err error) {
	return String(r.conn.Do("SPOP", key))
}

func (r *redis) SRandMember(key string, count int) (members []string, err error) {
	return redigo.Strings(r.conn.Do("SRANDMEMBER", key, count))
}

func (r *redis) SRem(key string, members ...interface{}) (removeNum int, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)
	args = append(args, key)
	args = append(args, members...)
	return redigo.Int(r.conn.Do("SREM", args...))
}

func (r *redis) SMove(sourceSetKey, destinationSetKey string, member interface{}) (success int, err error) {
	return redigo.Int(r.conn.Do("SMOVE", sourceSetKey, destinationSetKey, member))
}

func (r *redis) SDiff(keys ...interface{}) (members []string, err error) {
	return redigo.Strings(r.conn.Do("SDIFF", keys...))
}

func (r *redis) SDiffStore(destinationSetKey string, keys ...string) (memberCount int, err error) {
	var (
		args = make([]interface{}, 0, len(keys)+1)
	)
	args = append(args, destinationSetKey)
	for _, key := range keys {
		args = append(args, key)
	}
	return redigo.Int(r.conn.Do("SDIFFSTORE", args...))
}

func (r *redis) SInter(keys ...interface{}) (members []string, err error) {
	return redigo.Strings(r.conn.Do("SINTER", keys...))
}

func (r *redis) SInterStore(destinationSetKey string, keys ...string) (memberCount int, err error) {
	var (
		args = make([]interface{}, 0, len(keys)+1)
	)
	args = append(args, destinationSetKey)
	for _, key := range keys {
		args = append(args, key)
	}
	return redigo.Int(r.conn.Do("SINTERSTORE", args...))
}

func (r *redis) SUnion(keys ...interface{}) (members []string, err error) {
	return redigo.Strings(r.conn.Do("SUNION", keys...))
}

func (r *redis) SUnionStore(destinationSetKey string, keys ...string) (memberCount int, err error) {
	var (
		args = make([]interface{}, 0, len(keys)+1)
	)
	args = append(args, destinationSetKey)
	for _, key := range keys {
		args = append(args, key)
	}
	return redigo.Int(r.conn.Do("SUNIONSTORE", args...))
}

func (r *redis) SScan(key string, cursor int, match string, count int) (newCursor int, keys []string, err error) {
	var values []interface{}
	if match == "" {
		values, err = redigo.Values(r.conn.Do("SSCAN", key, cursor))
	} else if count == 0 {
		values, err = redigo.Values(r.conn.Do("SSCAN", key, cursor, "MATCH", match))
	} else {
		values, err = redigo.Values(r.conn.Do("SSCAN", key, cursor, "MATCH", match, "COUNT", count))
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

func (r *redis) ZAdd(key string, score, value interface{}, scoreAndValues ...interface{}) (createNum int, err error) {
	var (
		args = make([]interface{}, 0, len(scoreAndValues)+3)
	)
	args = append(args, key)
	args = append(args, score, value)
	for _, p := range scoreAndValues {
		args = append(args, p)
	}
	return redigo.Int(r.conn.Do("ZADD", args...))
}

func (r *redis) ZAddMap(key string, membersMap map[string]interface{}) (createNum int, err error) {
	var (
		args = make([]interface{}, 0, 2*len(membersMap)+1)
	)
	args = append(args, key)
	for value, score := range membersMap {
		args = append(args, score, value)
	}
	return redigo.Int(r.conn.Do("ZADD", args...))
}

func (r *redis) ZCard(key string) (count int, err error) {
	return redigo.Int(r.conn.Do("ZCARD", key))
}

func (r *redis) ZCount(key string, minScore, maxScore interface{}) (count int, err error) {
	return redigo.Int(r.conn.Do("ZCOUNT", key, minScore, maxScore))
}

func (r *redis) ZIncrBy(key string, increment interface{}, member string) (newScore string, err error) {
	return String(r.conn.Do("ZINCRBY", key, increment, member))
}

func (r *redis) ZRange(key string, startIndex, stopIndex int) (members []string, err error) {
	return redigo.Strings(r.conn.Do("ZRANGE", key, startIndex, stopIndex))
}

func (r *redis) ZRevRange(key string, startIndex, stopIndex int) (members []string, err error) {
	return redigo.Strings(r.conn.Do("ZREVRANGE", key, startIndex, stopIndex))
}

func (r *redis) ZRangeWithScore(key string, startIndex, stopIndex int) (members map[string]string, err error) {
	return redigo.StringMap(r.conn.Do("ZRANGE", key, startIndex, stopIndex, "WITHSCORES"))
}

func (r *redis) ZRevRangeWithScore(key string, startIndex, stopIndex int) (members map[string]string, err error) {
	return redigo.StringMap(r.conn.Do("ZREVRANGE", key, startIndex, stopIndex, "WITHSCORES"))
}

func (r *redis) ZRangeByScore(key string, minScore, maxScore interface{}, offset, limit int) (members []string, err error) {
	if limit == 0 {
		return redigo.Strings(r.conn.Do("ZRANGEBYSCORE", key, minScore, maxScore))
	}
	return redigo.Strings(r.conn.Do("ZRANGEBYSCORE", key, minScore, maxScore, "LIMIT", offset, limit))
}

func (r *redis) ZRevRangeByScore(key string, maxScore, minScore interface{}, offset, limit int) (members []string, err error) {
	if limit == 0 {
		return redigo.Strings(r.conn.Do("ZREVRANGEBYSCORE", key, maxScore, minScore))
	}
	return redigo.Strings(r.conn.Do("ZREVRANGEBYSCORE", key, maxScore, minScore, "LIMIT", offset, limit))
}

func (r *redis) ZRangeByScoreWithScore(key string, minScore, maxScore interface{}, offset, limit int) (members map[string]string, err error) {
	if limit == 0 {
		return redigo.StringMap(r.conn.Do("ZRANGEBYSCORE", key, minScore, maxScore, "WITHSCORES"))
	}
	return redigo.StringMap(r.conn.Do("ZRANGEBYSCORE", key, minScore, maxScore, "WITHSCORES", "LIMIT", offset, limit))
}

func (r *redis) ZRevRangeByScoreWithScore(key string, maxScore, minScore interface{}, offset, limit int) (members map[string]string, err error) {
	if limit == 0 {
		return redigo.StringMap(r.conn.Do("ZREVRANGEBYSCORE", key, maxScore, minScore, "WITHSCORES"))
	}
	return redigo.StringMap(r.conn.Do("ZREVRANGEBYSCORE", key, maxScore, minScore, "WITHSCORES", "LIMIT", offset, limit))
}

func (r *redis) ZRank(key, member string) (rankIndex int, err error) {
	return redigo.Int(r.conn.Do("ZRANK", key, member))
}

func (r *redis) ZRevRank(key, member string) (rankIndex int, err error) {
	return redigo.Int(r.conn.Do("ZREVRANK", key, member))
}

func (r *redis) ZScore(key, member string) (score string, err error) {
	return String(r.conn.Do("ZSCORE", key, member))
}

func (r *redis) ZRem(key string, members ...interface{}) (removeNum int, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)
	args = append(args, key)
	args = append(args, members...)
	return redigo.Int(r.conn.Do("ZREM", args))
}

func (r *redis) ZRemRangeByRank(key string, startIndex, stopIndex int) (removeNum int, err error) {
	return redigo.Int(r.conn.Do("ZREMRANGEBYRANK", key, startIndex, stopIndex))
}

func (r *redis) ZRemRangeByScore(key string, minScore, maxScore interface{}) (removeNum int, err error) {
	return redigo.Int(r.conn.Do("ZREMRANGEBYSCORE", key, minScore, maxScore))
}

func (r *redis) ZScan(key string, cursor int, match string, count int) (newCursor int, keys []string, err error) {
	var values []interface{}
	if match == "" {
		values, err = redigo.Values(r.conn.Do("ZSCAN", key, cursor))
	} else if count == 0 {
		values, err = redigo.Values(r.conn.Do("ZSCAN", key, cursor, "MATCH", match))
	} else {
		values, err = redigo.Values(r.conn.Do("ZSCAN", key, cursor, "MATCH", match, "COUNT", count))
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

//region 1.6 Pub/Sub

func (r *redis) Publish(channel string, msg string) (receiveNum int, err error) {
	return redigo.Int(r.conn.Do("PUBLISH", channel, msg))
}

func (r *redis) PubSubChannels(pattern string) (channels []string, err error) {
	return redigo.Strings(r.conn.Do("PUBSUB", "CHANNELS", pattern))
}

//endregion

//region 1.7 Script

func (r *redis) EvalOrSha(script string, keyCount int, keysAndArgs ...interface{}) (reply interface{}, err error) {
	s := redigo.NewScript(keyCount, script)
	return s.Do(r.conn, keysAndArgs...)
}

func (r *redis) EvalOrSha4Int64(script string, keyCount int, keysAndArgs ...interface{}) (res int64, err error) {
	s := redigo.NewScript(keyCount, script)
	return redigo.Int64(s.Do(r.conn, keysAndArgs...))
}

func (r *redis) EvalOrSha4String(script string, keyCount int, keysAndArgs ...interface{}) (res string, err error) {
	s := redigo.NewScript(keyCount, script)
	return String(s.Do(r.conn, keysAndArgs...))
}

//endregion

//region 1.8 Transaction

func (r *redis) Multi(kind uint8) (multi Multi, err error) {
	switch kind {
	case Transaction:
		return TransMulti(), nil
	case Pipeline:
		return PipeMulti(), nil
	}
	return nil, ErrInvalidMultiKind
}

func (r *redis) Exec(multi Multi) (values []interface{}, err error) {
	if multi.Kind() == Transaction {
		_ = r.conn.Send("MULTI")
	}

	for _, cmd := range multi.CmdList() {
		_ = r.conn.Send(cmd.cmd, cmd.args...)
	}

	if multi.Kind() == Pipeline {
		return redigo.Values(r.conn.Do(""))
	}

	ReleaseMulti(multi)
	return redigo.Values(r.conn.Do("EXEC"))
}

func (r *redis) Watch(keys ...interface{}) (ok bool, err error) {
	var res string
	res, err = String(r.conn.Do("WATCH", keys...))
	return res == Ok, err
}

func (r *redis) UnWatch(keys ...interface{}) (ok bool, err error) {
	var res string
	res, err = String(r.conn.Do("UNWATCH", keys...))
	return res == Ok, err
}

//endregion

//region 1.9 Server

func (r *redis) ClientList() (clients []string, err error) {
	var clientsStr string
	clientsStr, err = String(r.conn.Do("CLIENT", "LIST"))
	if err != nil {
		return nil, err
	}

	return strings.Split(clientsStr, "\n"), nil
}

func (r *redis) ConfigGet(pattern string) (conf map[string]string, err error) {
	return redigo.StringMap(r.conn.Do("CONFIG", "GET", pattern))
}

func (r *redis) ConfigSet(param string, value interface{}) (ok bool, err error) {
	var res string
	res, err = redigo.String(r.conn.Do("CONFIG", "SET", param, value))
	return res == Ok, err
}

//endregion
