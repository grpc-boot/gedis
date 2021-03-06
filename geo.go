package gedis

import (
	redigo "github.com/garyburd/redigo/redis"
)

type Position struct {
	Lng float64
	Lat float64
}

type Location struct {
	Member   string
	Distance string
	Hash     int64
	Lng      float64
	Lat      float64
}

//region 1.6 Geo

func (p *pool) GeoAdd(key string, longitude, latitude float64, member interface{}, args ...interface{}) (createNum int, err error) {
	var (
		params = make([]interface{}, 0, len(args)+4)
	)
	params = append(params, key, longitude, latitude, member)
	params = append(params, args...)
	return redigo.Int(p.Do("GEOADD", params...))
}

func (p *pool) GeoHash(key string, members ...interface{}) (hashList []string, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)

	args = append(args, key)
	args = append(args, members...)

	return redigo.Strings(p.Do("GEOHASH", args...))
}

func (p *pool) GeoDel(key string, members ...interface{}) (removeNum int, err error) {
	return p.ZRem(key, members)
}

func (p *pool) GeoDist(key string, member1, member2 interface{}, unit string) (distance string, err error) {
	return String(p.Do("GEODIST", key, member1, member2, unit))
}

func (p *pool) GeoPos(key string, members ...interface{}) (positionList []Position, err error) {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)

	args = append(args, key)
	args = append(args, members...)

	return Positions(p.Do("GEOPOS", args...))
}

func (p *pool) GeoRadius(key string, longitude, latitude float64, radius interface{}, unit string, count int, sort string) (locationList []Location, err error) {
	if sort == "" {
		sort = "ASC"
	}

	if count == 0 {
		return Locations(p.Do("GEORADIUS", key, longitude, latitude, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", sort))
	}

	return Locations(p.Do("GEORADIUS", key, longitude, latitude, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", "COUNT", count, sort))
}

func (p *pool) GeoRadiusByMember(key string, member interface{}, radius interface{}, unit string, count int, sort string) (locationList []Location, err error) {
	if sort == "" {
		sort = "ASC"
	}

	if count == 0 {
		return Locations(p.Do("GEORADIUSBYMEMBER", key, member, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", sort))
	}

	return Locations(p.Do("GEORADIUSBYMEMBER", key, member, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", "COUNT", count, sort))

}

//endregion

func (m *multi) GeoAdd(key string, longitude, latitude float64, member interface{}, args ...interface{}) Multi {
	var (
		params = make([]interface{}, 0, len(args)+4)
	)
	params = append(params, key, longitude, latitude, member)
	params = append(params, args...)
	m.cmdList = append(m.cmdList, Cmd{cmd: "GEOADD", args: params})
	return m
}

func (m *multi) GeoHash(key string, members ...interface{}) Multi {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)

	args = append(args, key)
	args = append(args, members...)

	m.cmdList = append(m.cmdList, Cmd{cmd: "GEOHASH", args: args})
	return m
}

func (m *multi) GeoDel(key string, members ...interface{}) Multi {
	return m.ZRem(key, members...)
}

func (m *multi) GeoDist(key string, member1, member2 interface{}, unit string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "GEODIST", args: []interface{}{key, member1, member2, unit}})
	return m
}

func (m *multi) GeoPos(key string, members ...interface{}) Multi {
	var (
		args = make([]interface{}, 0, len(members)+1)
	)

	args = append(args, key)
	args = append(args, members...)

	m.cmdList = append(m.cmdList, Cmd{cmd: "GEOPOS", args: args})
	return m
}

func (m *multi) GeoRadius(key string, longitude, latitude float64, radius interface{}, unit string, count int, sort string) Multi {
	if sort == "" {
		sort = "ASC"
	}

	var args []interface{}
	if count == 0 {
		args = []interface{}{key, longitude, latitude, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", sort}
	} else {
		args = []interface{}{key, longitude, latitude, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", "COUNT", count, sort}
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "GEORADIUS", args: args})
	return m
}

func (m *multi) GeoRadiusByMember(key string, member interface{}, radius interface{}, unit string, count int, sort string) Multi {
	if sort == "" {
		sort = "ASC"
	}

	var args []interface{}
	if count == 0 {
		args = []interface{}{key, member, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", sort}
	} else {
		args = []interface{}{key, member, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", "COUNT", count, sort}
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "GEORADIUSBYMEMBER", args: args})
	return m
}
