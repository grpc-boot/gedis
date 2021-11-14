package gedis

import (
	redigo "github.com/garyburd/redigo/redis"
)

const (
	Ok      = `OK`
	Success = 1
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
	Persist(key interface{}) (ok int, err error)
	Scan(cursor int, match string, count int) (newCursor int, keys []string, err error)
	Keys(pattern string) (keys []string, err error)
	Dump(key string) (serializedValue string, err error)
	Restore(key string, pttl int64, serializedValue string) (ok bool, err error)
	Move(key string, db int) (success int, err error)
	RandomKey() (key string, err error)
	ReName(key string, newKey string) (success bool, err error)
	ReNameNx(key string, newKey string) (success int, err error)
	Type(key string) (t string, err error)

	//-----------------String--------------------------
	Get(key string) (val string, err error)
	Set(key string, value interface{}, args ...interface{}) (ok bool, err error)
	SetEx(key string, seconds int, value interface{}) (ok bool, err error)
	Incr(key string) (val int64, err error)
	IncrBy(key string, increment int) (val int64, err error)
	IncrByFloat(key string, increment float64) (val float64, err error)
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

func (r *redis) Get(key string) (val string, err error) {
	return redigo.String(r.conn.Do("GET", key))
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

func (r *redis) Incr(key string) (val int64, err error) {
	return redigo.Int64(r.conn.Do("INCR", key))
}

func (r *redis) IncrBy(key string, increment int) (val int64, err error) {
	return redigo.Int64(r.conn.Do("INCRBY", key, increment))
}

func (r *redis) IncrByFloat(key string, increment float64) (val float64, err error) {
	return redigo.Float64(r.conn.Do("INCRBYFLOAT", key, increment))
}

//endregion
