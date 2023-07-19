package gedis

import (
	redigo "github.com/garyburd/redigo/redis"
)

// Position 经纬度位置
type Position struct {
	Lng float64
	Lat float64
}

// Location 位置
type Location struct {
	Member   string
	Distance string
	Hash     int64
	Lng      float64
	Lat      float64
}

//region 1.6 Geo

// GeoAdd redis命令
func (mp *myPool) GeoAdd(key string, longitude, latitude float64, member interface{}, args ...interface{}) (createNum int, err error) {
	var (
		params = make([]interface{}, len(args)+4)
	)

	params[0] = key
	params[1] = longitude
	params[2] = latitude
	params[3] = member

	for index, _ := range args {
		params[index+4] = args[index]
	}

	return redigo.Int(mp.Do("GEOADD", params...))
}

// GeoHash redis命令
func (mp *myPool) GeoHash(key string, members ...interface{}) (hashList []string, err error) {
	var (
		args = make([]interface{}, len(members)+1)
	)

	args[0] = key
	for index, _ := range members {
		args[index+1] = members[index]
	}

	return redigo.Strings(mp.Do("GEOHASH", args...))
}

// GeoDel redis命令
func (mp *myPool) GeoDel(key string, members ...interface{}) (removeNum int, err error) {
	return mp.ZRem(key, members)
}

// GeoDist redis命令
func (mp *myPool) GeoDist(key string, member1, member2 interface{}, unit string) (distance string, err error) {
	return String(mp.Do("GEODIST", key, member1, member2, unit))
}

// GeoPos redis命令
func (mp *myPool) GeoPos(key string, members ...interface{}) (positionList []Position, err error) {
	var (
		args = make([]interface{}, len(members)+1)
	)

	args[0] = key
	for index, _ := range members {
		args[index+1] = members[index]
	}

	return Positions(mp.Do("GEOPOS", args...))
}

// GeoRadius redis命令
func (mp *myPool) GeoRadius(key string, longitude, latitude float64, radius interface{}, unit string, count int, sort string) (locationList []Location, err error) {
	if sort == "" {
		sort = "ASC"
	}

	if count == 0 {
		return Locations(mp.Do("GEORADIUS", key, longitude, latitude, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", sort))
	}

	return Locations(mp.Do("GEORADIUS", key, longitude, latitude, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", "COUNT", count, sort))
}

// GeoRadiusByMember redis命令
func (mp *myPool) GeoRadiusByMember(key string, member interface{}, radius interface{}, unit string, count int, sort string) (locationList []Location, err error) {
	if sort == "" {
		sort = "ASC"
	}

	if count == 0 {
		return Locations(mp.Do("GEORADIUSBYMEMBER", key, member, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", sort))
	}

	return Locations(mp.Do("GEORADIUSBYMEMBER", key, member, radius, unit, "WITHCOORD", "WITHHASH", "WITHDIST", "COUNT", count, sort))

}

//endregion

// GeoAdd redis命令
func (m *multi) GeoAdd(key string, longitude, latitude float64, member interface{}, args ...interface{}) Multi {
	var (
		params = make([]interface{}, len(args)+4)
	)

	params[0] = key
	params[1] = longitude
	params[2] = latitude
	params[3] = member

	for index, _ := range args {
		params[index+4] = args[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "GEOADD", args: params})
	return m
}

// GeoHash redis命令
func (m *multi) GeoHash(key string, members ...interface{}) Multi {
	var (
		args = make([]interface{}, len(members)+1)
	)

	args[0] = key
	for index, _ := range members {
		args[index+1] = members[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "GEOHASH", args: args})
	return m
}

// GeoDel redis命令
func (m *multi) GeoDel(key string, members ...interface{}) Multi {
	return m.ZRem(key, members...)
}

// GeoDist redis命令
func (m *multi) GeoDist(key string, member1, member2 interface{}, unit string) Multi {
	m.cmdList = append(m.cmdList, Cmd{cmd: "GEODIST", args: []interface{}{key, member1, member2, unit}})
	return m
}

// GeoPos redis命令
func (m *multi) GeoPos(key string, members ...interface{}) Multi {
	var (
		args = make([]interface{}, len(members)+1)
	)

	args[0] = key
	for index, _ := range members {
		args[index+1] = members[index]
	}

	m.cmdList = append(m.cmdList, Cmd{cmd: "GEOPOS", args: args})
	return m
}

// GeoRadius redis命令
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

// GeoRadiusByMember redis命令
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
