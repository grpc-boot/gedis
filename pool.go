package gedis

import (
	redigo "github.com/garyburd/redigo/redis"
	"github.com/grpc-boot/base"
)

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
	HGetAllBytes(key string) (keyValues map[string][]byte, err error)
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
	EvalOrSha(script *redigo.Script, keysAndArgs ...interface{}) (reply interface{}, err error)
	EvalOrSha4Int64(script *redigo.Script, keysAndArgs ...interface{}) (res int64, err error)
	EvalOrSha4String(script *redigo.Script, keysAndArgs ...interface{}) (res string, err error)

	//--------------------Transaction---------------------------
	Exec(multi Multi) (values []interface{}, err error)

	//--------------------Lock/Limit/Cache---------------------------
	Acquire(key string, timeoutSecond int) (token int64, err error)
	Release(key string, token int64) (ok bool, err error)
	CacheGet(key string, current, timeoutSecond int64, handler Handler) (value []byte, err error)
	CacheGetItem(key string, current, timeoutSecond int64, handler Handler) (item Item, err error)
	CacheRemove(key string) (ok bool, err error)
	GetToken(key string, current int64, capacity, rate, reqNum, keyTimeoutSecond int) (ok bool, err error)
	SecondLimitByToken(key string, limit int, reqNum int) (ok bool, err error)
	SecondLimitByTime(key string, limit int, reqNum int) (ok bool, err error)
	MinuteLimitByTime(key string, limit int, reqNum int) (ok bool, err error)
	HourLimitByTime(key string, limit int, reqNum int) (ok bool, err error)
	DayLimitByTime(key string, limit int, reqNum int) (ok bool, err error)

	//-----------------Server--------------------------
	ClientList() (clients []string, err error)
	ConfigGet(pattern string) (conf map[string]string, err error)
	ConfigSet(param string, value interface{}) (ok bool, err error)
}
