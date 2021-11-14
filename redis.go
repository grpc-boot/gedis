package gedis

import (
	redigo "github.com/garyburd/redigo/redis"
)

type Redis interface {
	Close() (err error)
	Del(keys ...interface{}) (delNum int, err error)
	Exists(key interface{}) (exists int, err error)
	Expire(key interface{}, second int64) (ok int, err error)
	ExpireAt(key interface{}, unixTime int64) (ok int, err error)
	Ttl(key interface{}) (second int64, err error)
	Persist(key interface{}) (ok int, err error)
	Scan(cursor int, match string, count int) (newCursor int, keys []string, err error)
}

func newRedis(conn redigo.Conn) Redis {
	return &redis{conn: conn}
}

type redis struct {
	conn redigo.Conn
}

func (r *redis) Close() (err error) {
	return r.conn.Close()
}

func (r *redis) Del(keys ...interface{}) (delNum int, err error) {
	return redigo.Int(r.conn.Do("DEL", keys...))
}

func (r *redis) Exists(key interface{}) (exists int, err error) {
	return redigo.Int(r.conn.Do("EXISTS", key))
}

func (r *redis) Expire(key interface{}, second int64) (ok int, err error) {
	return redigo.Int(r.conn.Do("EXPIRE", key, second))
}

func (r *redis) ExpireAt(key interface{}, unixTime int64) (ok int, err error) {
	return redigo.Int(r.conn.Do("EXPIREAT", key, unixTime))
}

func (r *redis) Ttl(key interface{}) (second int64, err error) {
	return redigo.Int64(r.conn.Do("TTL", key))
}

func (r *redis) Persist(key interface{}) (ok int, err error) {
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
