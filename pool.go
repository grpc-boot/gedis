package gedis

import (
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/grpc-boot/base"
)

const (
	Ok      = `OK`
	Success = 1
)

const (
	lockFormat = "ged_L:%s"
	delLock    = `if redis.call('get', KEYS[1]) == ARGV[1]
            then
                return redis.call('del', KEYS[1])
            end
            return 0`
)

type Option struct {
	Host string `yaml:"host" json:"host"`
	Port int    `yaml:"port" json:"port"`
	Auth string `yaml:"auth" json:"auth"`
	Db   uint8  `yaml:"db" json:"db"`
	//单位s
	MaxConnLifetime int  `yaml:"maxConnLifetime" json:"maxConnLifetime"`
	MaxIdle         int  `yaml:"maxIdle" json:"maxIdle"`
	MaxActive       int  `yaml:"maxActive" json:"maxActive"`
	Wait            bool `yaml:"wait" json:"wait"`
	//单位ms
	ConnectTimeout int `yaml:"connectTimeout" json:"connectTimeout"`
	//单位ms
	ReadTimeout int `yaml:"readTimeout" json:"readTimeout"`
	//单位ms
	WriteTimeout int `yaml:"writeTimeout" json:"writeTimeout"`
	//节点索引
	Index int `yaml:"index" json:"index"`
}

type Pool interface {
	base.CanHash

	ActiveCount() (num int)
	IdleCount() (num int)
	Stats() redigo.PoolStats
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

	//----------------------Geo-----------------------------
	GeoAdd(key string, longitude, latitude float64, member interface{}, args ...interface{}) (createNum int, err error)
	GeoHash(key string, members ...interface{}) (hashList []string, err error)
	GeoDel(key string, members ...interface{}) (removeNum int, err error)
	GeoDist(key string, member1, member2 interface{}, unit string) (distance string, err error)
	GeoPos(key string, members ...interface{}) (positionList []Position, err error)
	GeoRadius(key string, longitude, latitude float64, radius interface{}, unit string, count int, sort string) (locationList []Location, err error)
	GeoRadiusByMember(key string, member interface{}, radius interface{}, unit string, count int, sort string) (locationList []Location, err error)

	//--------------------Pub/Sub---------------------------
	Publish(channel string, msg string) (receiveNum int, err error)
	PubSubChannels(pattern string) (channels []string, err error)

	//--------------------Script---------------------------
	EvalOrSha(script string, keyCount int, keysAndArgs ...interface{}) (reply interface{}, err error)
	EvalOrSha4Int64(script string, keyCount int, keysAndArgs ...interface{}) (res int64, err error)
	EvalOrSha4String(script string, keyCount int, keysAndArgs ...interface{}) (res string, err error)

	//--------------------Transaction---------------------------
	Exec(multi Multi) (values []interface{}, err error)

	//--------------------Lock/Cache---------------------------
	Acquire(key string, timeoutSecond int) (token int64, err error)
	Release(key string, token int64) (ok bool, err error)
	CacheGet(key string, timeoutSecond int64, handler func() []byte) (item Item, err error)
	CacheRemove(key string) (ok bool, err error)

	//-----------------Server--------------------------
	ClientList() (clients []string, err error)
	ConfigGet(pattern string) (conf map[string]string, err error)
	ConfigSet(param string, value interface{}) (ok bool, err error)
}

type pool struct {
	Pool

	pool *redigo.Pool
	id   []byte
}

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
			return err
		},
	}

	if option.MaxConnLifetime > 0 {
		pl.MaxConnLifetime = time.Second * time.Duration(option.MaxConnLifetime)
	}

	return &pool{
		pool: pl,
		id:   []byte(fmt.Sprintf("%s:%d-%d", option.Host, option.Port, option.Index)),
	}
}

func (p *pool) HashCode() uint32 {
	return crc32.ChecksumIEEE(p.id)
}

func (p *pool) ActiveCount() (num int) {
	return p.pool.ActiveCount()
}

func (p *pool) IdleCount() (num int) {
	return p.pool.IdleCount()
}

func (p *pool) Stats() redigo.PoolStats {
	return p.pool.Stats()
}

func (p *pool) Close() (err error) {
	return p.pool.Close()
}

func (p *pool) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
	r := p.pool.Get()
	defer r.Close()
	return r.Do(cmd, args...)
}

//region 1.0 Key

func (p *pool) Del(keys ...interface{}) (delNum int, err error) {
	return redigo.Int(p.Do("DEL", keys...))
}

func (p *pool) Exists(key interface{}) (exists bool, err error) {
	var suc int
	suc, err = redigo.Int(p.Do("EXISTS", key))
	return suc == Success, err
}

func (p *pool) Expire(key interface{}, second int64) (success int, err error) {
	return redigo.Int(p.Do("EXPIRE", key, second))
}

func (p *pool) ExpireAt(key interface{}, unixTime int64) (success int, err error) {
	return redigo.Int(p.Do("EXPIREAT", key, unixTime))
}

func (p *pool) Ttl(key interface{}) (second int64, err error) {
	return redigo.Int64(p.Do("TTL", key))
}

func (p *pool) Persist(key interface{}) (success int, err error) {
	return redigo.Int(p.Do("PERSIST", key))
}

func (p *pool) Scan(cursor int, match string, count int) (newCursor int, keys []string, err error) {
	var values []interface{}
	if match == "" {
		values, err = redigo.Values(p.Do("SCAN", cursor))
	} else if count == 0 {
		values, err = redigo.Values(p.Do("SCAN", cursor, "MATCH", match))
	} else {
		values, err = redigo.Values(p.Do("SCAN", cursor, "MATCH", match, "COUNT", count))
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

func (p *pool) Keys(pattern string) (keys []string, err error) {
	return redigo.Strings(p.Do("KEYS", pattern))
}

func (p *pool) Dump(key string) (serializedValue string, err error) {
	return String(p.Do("DUMP", key))
}

func (p *pool) Restore(key string, pttl int64, serializedValue string) (ok bool, err error) {
	var res string
	res, err = redigo.String(p.Do("RESTORE", key, pttl, serializedValue))
	return res == Ok, err
}

func (p *pool) Move(key string, db int) (success int, err error) {
	return redigo.Int(p.Do("MOVE", key, db))
}

func (p *pool) RandomKey() (key string, err error) {
	return String(p.Do("RANDOMKEY"))
}

func (p *pool) ReName(key string, newKey string) (ok bool, err error) {
	var res string
	res, err = redigo.String(p.Do("RENAME", key, newKey))
	return res == Ok, err
}

func (p *pool) ReNameNx(key string, newKey string) (success int, err error) {
	return redigo.Int(p.Do("RENAMENX", key, newKey))
}

func (p *pool) Type(key string) (t string, err error) {
	return redigo.String(p.Do("TYPE", key))
}

//endregion

//region 1.1 String

func (p *pool) Append(key string, value interface{}) (strLength int, err error) {
	return redigo.Int(p.Do("APPEND", key, value))
}

func (p *pool) StrLen(key string) (strLength int, err error) {
	return redigo.Int(p.Do("STRLEN", key))
}

func (p *pool) Get(key string) (val string, err error) {
	return redigo.String(p.Do("GET", key))
}

func (p *pool) MGet(keys ...string) (values []string, err error) {
	var (
		args = make([]interface{}, 0, len(keys))
	)

	for _, key := range keys {
		args = append(args, key)
	}

	return redigo.Strings(p.Do("MGET", args...))
}

func (p *pool) MGetMap(keys ...string) (keyValue map[string]string, err error) {
	var (
		values []string
		args   = make([]interface{}, 0, len(keys))
	)
	for _, key := range keys {
		args = append(args, key)
	}
	values, err = redigo.Strings(p.Do("MGET", args...))
	if err != nil {
		return nil, err
	}

	keyValue = make(map[string]string, len(values))
	for index, key := range keys {
		keyValue[key] = values[index]
	}
	return
}

func (p *pool) MSet(key1 string, value1 interface{}, keyValues ...interface{}) (ok bool, err error) {
	var (
		res  string
		args = make([]interface{}, 0, len(keyValues)+2)
	)

	args = append(args, key1, value1)
	args = append(args, keyValues...)

	res, err = redigo.String(p.Do("MSET", args...))
	return res == Ok, err
}

func (p *pool) MSetByMap(keyValues map[string]interface{}) (ok bool, err error) {
	var (
		res  string
		args = make([]interface{}, 0, 2*len(keyValues))
	)

	for key, value := range keyValues {
		args = append(args, key, value)
	}

	res, err = redigo.String(p.Do("MSET", args...))
	return res == Ok, err
}

func (p *pool) Set(key string, value interface{}, args ...interface{}) (ok bool, err error) {
	var (
		res    string
		params = make([]interface{}, 0, len(args)+2)
	)
	params = append(params, key, value)
	params = append(params, args...)
	res, err = String(p.Do("SET", params...))
	return res == Ok, err
}

func (p *pool) SetEx(key string, seconds int, value interface{}) (ok bool, err error) {
	var res string
	res, err = redigo.String(p.Do("SETEX", key, seconds, value))
	return res == Ok, err
}

func (p *pool) SetNx(key string, value interface{}) (ok int, err error) {
	return redigo.Int(p.Do("SETNX", key, value))
}

func (p *pool) GetSet(key string, value interface{}) (oldValue string, err error) {
	return String(p.Do("GETSET", key, value))
}

func (p *pool) Incr(key string) (val int64, err error) {
	return redigo.Int64(p.Do("INCR", key))
}

func (p *pool) Decr(key string) (val int64, err error) {
	return redigo.Int64(p.Do("DECR", key))
}

func (p *pool) IncrBy(key string, increment int) (val int64, err error) {
	return redigo.Int64(p.Do("INCRBY", key, increment))
}

func (p *pool) DecrBy(key string, decrement int) (val int64, err error) {
	return redigo.Int64(p.Do("DECRBY", key, decrement))
}

func (p *pool) IncrByFloat(key string, increment float64) (val float64, err error) {
	return redigo.Float64(p.Do("INCRBYFLOAT", key, increment))
}

func (p *pool) SetRange(key string, offset int, val string) (strLength int, err error) {
	return redigo.Int(p.Do("SETRANGE", key, offset, val))
}

func (p *pool) GetRange(key string, start, end int) (val string, err error) {
	return String(p.Do("GETRANGE", key, start, end))
}

func (p *pool) SetBit(key string, offset int, bit int8) (oldBit int, err error) {
	return redigo.Int(p.Do("SETBIT", key, offset, bit))
}

func (p *pool) GetBit(key string, offset int) (bit int, err error) {
	return redigo.Int(p.Do("GETBIT", key, offset))
}

func (p *pool) BitCount(key string, args ...interface{}) (num int, err error) {
	params := make([]interface{}, 0, len(args)+1)
	params = append(params, key)
	params = append(params, args...)
	return redigo.Int(p.Do("BITCOUNT", params...))
}

//endregion

//region 1.2 Hash

func (p *pool) HSet(key string, field string, value interface{}) (isNew int, err error) {
	return redigo.Int(p.Do("HSET", key, field, value))
}

func (p *pool) HSetNx(key string, field string, value interface{}) (ok int, err error) {
	return redigo.Int(p.Do("HSETNX", key, field, value))
}

func (p *pool) HGet(key string, field string) (value string, err error) {
	return String(p.Do("HGET", key, field))
}

func (p *pool) HMSet(key string, field1 string, value1 interface{}, args ...interface{}) (ok bool, err error) {
	var (
		res    string
		params = make([]interface{}, 0, len(args)+3)
	)

	params = append(params, key, field1, value1)
	params = append(params, args...)

	res, err = redigo.String(p.Do("HMSET", params...))
	return res == Ok, err
}

func (p *pool) HMSetMap(key string, keyValues map[string]interface{}) (ok bool, err error) {
	var (
		res  string
		args = make([]interface{}, 0, 2*len(keyValues)+1)
	)

	args = append(args, key)
	for k, v := range keyValues {
		args = append(args, k, v)
	}

	res, err = redigo.String(p.Do("HMSET", args...))
	return res == Ok, err
}

func (p *pool) HMGet(key string, fields ...string) (values []string, err error) {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}
	return redigo.Strings(p.Do("HMGET", args...))
}

func (p *pool) HMGetMap(key string, fields ...string) (keyValues map[string]string, err error) {
	var (
		values []string
		args   = make([]interface{}, 0, len(fields)+1)
	)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}
	values, err = redigo.Strings(p.Do("HMGET", args...))
	if err != nil {
		return nil, err
	}

	keyValues = make(map[string]string, len(fields))
	for index, field := range fields {
		keyValues[field] = values[index]
	}

	return
}

func (p *pool) HGetAll(key string) (keyValues map[string]string, err error) {
	return redigo.StringMap(p.Do("HGETALL", key))
}

func (p *pool) HDel(key string, fields ...string) (delNum int, err error) {
	var (
		args = make([]interface{}, 0, len(fields)+1)
	)
	args = append(args, key)
	for _, field := range fields {
		args = append(args, field)
	}
	return redigo.Int(p.Do("HDEL", args...))
}

func (p *pool) HExists(key string, field string) (exists bool, err error) {
	var suc int
	suc, err = redigo.Int(p.Do("HEXISTS", key, field))
	return suc == Success, err
}

func (p *pool) HIncrBy(key string, field string, increment int) (val int64, err error) {
	return redigo.Int64(p.Do("HINCRBY", key, field, increment))
}

func (p *pool) HIncrByFloat(key string, field string, increment float64) (val float64, err error) {
	return redigo.Float64(p.Do("HINCRBYFLOAT", key, field, increment))
}

func (p *pool) HKeys(key string) (fields []string, err error) {
	return redigo.Strings(p.Do("HKEYS", key))
}

func (p *pool) HVals(key string) (values []string, err error) {
	return redigo.Strings(p.Do("HVALS", key))
}

func (p *pool) HLen(key string) (length int, err error) {
	return redigo.Int(p.Do("HLEN", key))
}

//endregion

//region 1.3 List

func (p *pool) LLen(key string) (listLength int, err error) {
	return redigo.Int(p.Do("LLEN", key))
}

func (p *pool) LPush(key string, values ...interface{}) (listLength int, err error) {
	var (
		args = make([]interface{}, 0, len(values)+1)
	)
	args = append(args, key)
	for _, value := range values {
		args = append(args, value)
	}
	return redigo.Int(p.Do("LPUSH", args...))
}

func (p *pool) LPushX(key string, value interface{}) (listLength int, err error) {
	return redigo.Int(p.Do("LPUSHX", key, value))
}

func (p *pool) LPop(key string) (value string, err error) {
	return String(p.Do("LPOP", key))
}

func (p *pool) LIndex(key string, index int) (value string, err error) {
	return String(p.Do("LINDEX", key, index))
}

func (p *pool) LRange(key string, start, stop int) (values []string, err error) {
	return redigo.Strings(p.Do("LRANGE", key, start, stop))
}

func (p *pool) LSet(key string, index int, value interface{}) (ok bool, err error) {
	var suc string
	suc, err = String(p.Do("LSET", key, index, value))
	return suc == Ok, err
}

func (p *pool) LTrim(key string, start, stop int) (ok bool, err error) {
	var suc string
	suc, err = String(p.Do("LTRIM", key, start, stop))
	return suc == Ok, err
}

func (p *pool) RPush(key string, values ...interface{}) (listLength int, err error) {
	var (
		args = make([]interface{}, 0, len(values)+1)
	)
	args = append(args, key)
	for _, value := range values {
		args = append(args, value)
	}
	return redigo.Int(p.Do("RPUSH", args...))
}

func (p *pool) RPushX(key string, value interface{}) (listLength int, err error) {
	return redigo.Int(p.Do("RPUSHX", key, value))
}

func (p *pool) RPop(key string) (value string, err error) {
	return String(p.Do("RPOP", key))
}

//endregion

//region 1.4 Set

func (p *pool) SAdd(key string, members ...interface{}) (addNum int, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)
	args = append(args, key)
	args = append(args, members...)
	return redigo.Int(p.Do("SADD", args...))
}

func (p *pool) SMembers(key string) (members []string, err error) {
	return redigo.Strings(p.Do("SMEMBERS", key))
}

func (p *pool) SIsMember(key string, member interface{}) (exists bool, err error) {
	var suc int
	suc, err = redigo.Int(p.Do("SISMEMBER", key, member))
	return suc == Success, err
}

func (p *pool) SCard(key string) (count int, err error) {
	return redigo.Int(p.Do("SCARD", key))
}

func (p *pool) SPop(key string) (member string, err error) {
	return String(p.Do("SPOP", key))
}

func (p *pool) SRandMember(key string, count int) (members []string, err error) {
	return redigo.Strings(p.Do("SRANDMEMBER", key, count))
}

func (p *pool) SRem(key string, members ...interface{}) (removeNum int, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)
	args = append(args, key)
	args = append(args, members...)
	return redigo.Int(p.Do("SREM", args...))
}

func (p *pool) SMove(sourceSetKey, destinationSetKey string, member interface{}) (success int, err error) {
	return redigo.Int(p.Do("SMOVE", sourceSetKey, destinationSetKey, member))
}

func (p *pool) SDiff(keys ...interface{}) (members []string, err error) {
	return redigo.Strings(p.Do("SDIFF", keys...))
}

func (p *pool) SDiffStore(destinationSetKey string, keys ...string) (memberCount int, err error) {
	var (
		args = make([]interface{}, 0, len(keys)+1)
	)
	args = append(args, destinationSetKey)
	for _, key := range keys {
		args = append(args, key)
	}
	return redigo.Int(p.Do("SDIFFSTORE", args...))
}

func (p *pool) SInter(keys ...interface{}) (members []string, err error) {
	return redigo.Strings(p.Do("SINTER", keys...))
}

func (p *pool) SInterStore(destinationSetKey string, keys ...string) (memberCount int, err error) {
	var (
		args = make([]interface{}, 0, len(keys)+1)
	)
	args = append(args, destinationSetKey)
	for _, key := range keys {
		args = append(args, key)
	}
	return redigo.Int(p.Do("SINTERSTORE", args...))
}

func (p *pool) SUnion(keys ...interface{}) (members []string, err error) {
	return redigo.Strings(p.Do("SUNION", keys...))
}

func (p *pool) SUnionStore(destinationSetKey string, keys ...string) (memberCount int, err error) {
	var (
		args = make([]interface{}, 0, len(keys)+1)
	)
	args = append(args, destinationSetKey)
	for _, key := range keys {
		args = append(args, key)
	}
	return redigo.Int(p.Do("SUNIONSTORE", args...))
}

func (p *pool) SScan(key string, cursor int, match string, count int) (newCursor int, keys []string, err error) {
	var values []interface{}
	if match == "" {
		values, err = redigo.Values(p.Do("SSCAN", key, cursor))
	} else if count == 0 {
		values, err = redigo.Values(p.Do("SSCAN", key, cursor, "MATCH", match))
	} else {
		values, err = redigo.Values(p.Do("SSCAN", key, cursor, "MATCH", match, "COUNT", count))
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

func (p *pool) ZAdd(key string, score, value interface{}, scoreAndValues ...interface{}) (createNum int, err error) {
	var (
		args = make([]interface{}, 0, len(scoreAndValues)+3)
	)
	args = append(args, key)
	args = append(args, score, value)
	for _, p := range scoreAndValues {
		args = append(args, p)
	}
	return redigo.Int(p.Do("ZADD", args...))
}

func (p *pool) ZAddMap(key string, membersMap map[string]interface{}) (createNum int, err error) {
	var (
		args = make([]interface{}, 0, 2*len(membersMap)+1)
	)
	args = append(args, key)
	for value, score := range membersMap {
		args = append(args, score, value)
	}
	return redigo.Int(p.Do("ZADD", args...))
}

func (p *pool) ZCard(key string) (count int, err error) {
	return redigo.Int(p.Do("ZCARD", key))
}

func (p *pool) ZCount(key string, minScore, maxScore interface{}) (count int, err error) {
	return redigo.Int(p.Do("ZCOUNT", key, minScore, maxScore))
}

func (p *pool) ZIncrBy(key string, increment interface{}, member string) (newScore string, err error) {
	return String(p.Do("ZINCRBY", key, increment, member))
}

func (p *pool) ZRange(key string, startIndex, stopIndex int) (members []string, err error) {
	return redigo.Strings(p.Do("ZRANGE", key, startIndex, stopIndex))
}

func (p *pool) ZRevRange(key string, startIndex, stopIndex int) (members []string, err error) {
	return redigo.Strings(p.Do("ZREVRANGE", key, startIndex, stopIndex))
}

func (p *pool) ZRangeWithScore(key string, startIndex, stopIndex int) (members map[string]string, err error) {
	return redigo.StringMap(p.Do("ZRANGE", key, startIndex, stopIndex, "WITHSCORES"))
}

func (p *pool) ZRevRangeWithScore(key string, startIndex, stopIndex int) (members map[string]string, err error) {
	return redigo.StringMap(p.Do("ZREVRANGE", key, startIndex, stopIndex, "WITHSCORES"))
}

func (p *pool) ZRangeByScore(key string, minScore, maxScore interface{}, offset, limit int) (members []string, err error) {
	if limit == 0 {
		return redigo.Strings(p.Do("ZRANGEBYSCORE", key, minScore, maxScore))
	}
	return redigo.Strings(p.Do("ZRANGEBYSCORE", key, minScore, maxScore, "LIMIT", offset, limit))
}

func (p *pool) ZRevRangeByScore(key string, maxScore, minScore interface{}, offset, limit int) (members []string, err error) {
	if limit == 0 {
		return redigo.Strings(p.Do("ZREVRANGEBYSCORE", key, maxScore, minScore))
	}
	return redigo.Strings(p.Do("ZREVRANGEBYSCORE", key, maxScore, minScore, "LIMIT", offset, limit))
}

func (p *pool) ZRangeByScoreWithScore(key string, minScore, maxScore interface{}, offset, limit int) (members map[string]string, err error) {
	if limit == 0 {
		return redigo.StringMap(p.Do("ZRANGEBYSCORE", key, minScore, maxScore, "WITHSCORES"))
	}
	return redigo.StringMap(p.Do("ZRANGEBYSCORE", key, minScore, maxScore, "WITHSCORES", "LIMIT", offset, limit))
}

func (p *pool) ZRevRangeByScoreWithScore(key string, maxScore, minScore interface{}, offset, limit int) (members map[string]string, err error) {
	if limit == 0 {
		return redigo.StringMap(p.Do("ZREVRANGEBYSCORE", key, maxScore, minScore, "WITHSCORES"))
	}
	return redigo.StringMap(p.Do("ZREVRANGEBYSCORE", key, maxScore, minScore, "WITHSCORES", "LIMIT", offset, limit))
}

func (p *pool) ZRank(key, member string) (rankIndex int, err error) {
	return redigo.Int(p.Do("ZRANK", key, member))
}

func (p *pool) ZRevRank(key, member string) (rankIndex int, err error) {
	return redigo.Int(p.Do("ZREVRANK", key, member))
}

func (p *pool) ZScore(key, member string) (score string, err error) {
	return String(p.Do("ZSCORE", key, member))
}

func (p *pool) ZRem(key string, members ...interface{}) (removeNum int, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)
	args = append(args, key)
	args = append(args, members...)
	return redigo.Int(p.Do("ZREM", args))
}

func (p *pool) ZRemRangeByRank(key string, startIndex, stopIndex int) (removeNum int, err error) {
	return redigo.Int(p.Do("ZREMRANGEBYRANK", key, startIndex, stopIndex))
}

func (p *pool) ZRemRangeByScore(key string, minScore, maxScore interface{}) (removeNum int, err error) {
	return redigo.Int(p.Do("ZREMRANGEBYSCORE", key, minScore, maxScore))
}

func (p *pool) ZScan(key string, cursor int, match string, count int) (newCursor int, keys []string, err error) {
	var values []interface{}
	if match == "" {
		values, err = redigo.Values(p.Do("ZSCAN", key, cursor))
	} else if count == 0 {
		values, err = redigo.Values(p.Do("ZSCAN", key, cursor, "MATCH", match))
	} else {
		values, err = redigo.Values(p.Do("ZSCAN", key, cursor, "MATCH", match, "COUNT", count))
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

func (p *pool) Publish(channel string, msg string) (receiveNum int, err error) {
	return redigo.Int(p.Do("PUBLISH", channel, msg))
}

func (p *pool) PubSubChannels(pattern string) (channels []string, err error) {
	return redigo.Strings(p.Do("PUBSUB", "CHANNELS", pattern))
}

//endregion

//region 1.8 Script

func (p *pool) EvalOrSha(script string, keyCount int, keysAndArgs ...interface{}) (reply interface{}, err error) {
	r := p.pool.Get()
	defer r.Close()
	s := redigo.NewScript(keyCount, script)
	return s.Do(r, keysAndArgs...)
}

func (p *pool) EvalOrSha4Int64(script string, keyCount int, keysAndArgs ...interface{}) (res int64, err error) {
	r := p.pool.Get()
	defer r.Close()
	s := redigo.NewScript(keyCount, script)
	return redigo.Int64(s.Do(r, keysAndArgs...))
}

func (p *pool) EvalOrSha4String(script string, keyCount int, keysAndArgs ...interface{}) (res string, err error) {
	r := p.pool.Get()
	defer r.Close()
	s := redigo.NewScript(keyCount, script)
	return String(s.Do(r, keysAndArgs...))
}

//endregion

//region 1.9 Transaction

func (p *pool) Exec(multi Multi) (values []interface{}, err error) {
	r := p.pool.Get()
	defer r.Close()

	if multi.Kind() == Transaction {
		_ = r.Send("MULTI")
	}

	for _, cmd := range multi.CmdList() {
		_ = r.Send(cmd.cmd, cmd.args...)
	}

	if multi.Kind() == Pipeline {
		return redigo.Values(r.Do(""))
	}

	ReleaseMulti(multi)
	return redigo.Values(r.Do("EXEC"))
}

//endregion

//region 1.10 Lock

func (p *pool) Acquire(key string, timeoutSecond int) (token int64, err error) {
	var (
		ok bool
	)

	token = time.Now().UnixNano()
	ok, err = p.Set(fmt.Sprintf(lockFormat, key), token, "NX", "EX", timeoutSecond)
	if !ok {
		return 0, err
	}
	return token, nil
}

func (p *pool) Release(key string, token int64) (ok bool, err error) {
	var (
		suc int64
	)
	suc, err = p.EvalOrSha4Int64(delLock, 1, fmt.Sprintf(lockFormat, key), token)
	return suc == 1, err
}

//endregion

//region 2.0 Server

func (p *pool) ClientList() (clients []string, err error) {
	var clientsStr string
	clientsStr, err = String(p.Do("CLIENT", "LIST"))
	if err != nil {
		return nil, err
	}

	return strings.Split(clientsStr, "\n"), nil
}

func (p *pool) ConfigGet(pattern string) (conf map[string]string, err error) {
	return redigo.StringMap(p.Do("CONFIG", "GET", pattern))
}

func (p *pool) ConfigSet(param string, value interface{}) (ok bool, err error) {
	var res string
	res, err = redigo.String(p.Do("CONFIG", "SET", param, value))
	return res == Ok, err
}

//endregion
