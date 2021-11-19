package gedis

import (
	redigo "github.com/garyburd/redigo/redis"
	"github.com/grpc-boot/base"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var (
	option = Option{
		Host:            "127.0.0.1",
		Port:            6379,
		Auth:            "",
		Db:              0,
		MaxConnLifetime: 600,
		MaxIdle:         10,
		MaxActive:       20,
		Wait:            false,
		ConnectTimeout:  3,
		ReadTimeout:     200,
		WriteTimeout:    500,
	}

	default_pl   Pool
	g            Group
	groupOptions GOption
)

type GOption struct {
	Group []GroupOption `yaml:"redis" json:"redis"`
}

func init() {
	default_pl = NewPool(option)
	err := base.YamlDecodeFile("./app.yml", &groupOptions)
	if err != nil {
		log.Fatal(err)
	}

	g, err = NewGroup(groupOptions.Group...)
	if err != nil {
		base.RedFatal(err.Error())
	}
}

func TestGroup_Range(t *testing.T) {
	g.Range(func(index int, p Pool, hitCount uint64) (handled bool) {
		t.Logf("index:%d, hashCode:%d, hitCount:%d", index, p.HashCode(), hitCount)
		return
	})
}

func TestRedis_Scan(t *testing.T) {
	newCursor, v, err := default_pl.Scan(0, "*s*", 10)
	t.Log(newCursor, v, err)
}

func TestRedis_Dump(t *testing.T) {
	val, err := default_pl.Dump(`tests`)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(val)
}

func TestRedis_Keys(t *testing.T) {
	keys, err := default_pl.Keys("*")
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(keys)
}

func TestRedis_RandomKey(t *testing.T) {
	key, err := default_pl.RandomKey()
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(key)
}

func TestRedis_Type(t *testing.T) {
	key, err := default_pl.Type(`test`)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(key)
}

func TestRedis_Get(t *testing.T) {
	ok, err := default_pl.Set(`test`, time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("want true, got false")
	}

	val, err := default_pl.Get(`test`)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(val)
}

func TestRedis_IncrByFloat(t *testing.T) {
	val, err := default_pl.Incr(`incr-test`)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("incr test:%d", val)

	val, err = default_pl.IncrBy(`incr-test`, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("incr by test:%d", val)

	v, err := default_pl.IncrByFloat(`incr-test-float`, 101.34)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("incr by test:%v", v)
}

func TestRedis_GetRange(t *testing.T) {
	var (
		key = `test_range`
	)

	val, err := default_pl.GetRange(key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("get range:%s", val)

	length, err := default_pl.SetRange(key, 15, time.Now().String())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("after setRange length:%d", length)

	val, err = default_pl.GetRange(key, 15, 20)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("get range [15-20]:%s", val)
}

func TestRedis_BitCount(t *testing.T) {
	var (
		key = `test_bit`
	)

	v, err := default_pl.GetBit(key, 1024)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(v)

	num, err := default_pl.BitCount(key)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(num)

	ok, err := default_pl.SetBit(key, 1024, 1)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(ok)

	num, err = default_pl.BitCount(key)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(num)

	num, err = default_pl.BitCount(key, 256, 512)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(num)
}

func TestRedis_MGet(t *testing.T) {
	var keys = []string{
		"mget0",
		"mget1",
		"mget2",
		"mget3",
		"mget4",
		"mget5",
		"mget6",
		"mget7",
		"mget8",
		"mget9",
		"mget10",
	}

	values, err := default_pl.MGet(keys...)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("mget values:%v", values)

	_, err = default_pl.SetEx(keys[0], 60, time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}

	ok, err := default_pl.SetNx(keys[3], time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("set nx:%v", ok)

	kv, err := default_pl.MGetMap(keys...)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("mgetmap:%v", kv)

	suc, err := default_pl.MSetByMap(map[string]interface{}{
		keys[2]: time.Now().UnixNano(),
		keys[5]: rand.Int63n(time.Now().Unix()),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("msetmap:%v", suc)

	values, err = default_pl.MGet(keys...)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("mget:%+v", values)

	suc, err = default_pl.MSet(keys[6], time.Now().UnixNano(), keys[7], time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("mset:%v", suc)
}

func TestRedis_HGetAll(t *testing.T) {
	var (
		key    = `test_hash_opt`
		fields = []string{
			`field0`,
			`field1`,
			`field2`,
			`field3`,
			`field4`,
			`field5`,
			`field6`,
			`field7`,
			`field8`,
			`field9`,
		}
	)

	ok, err := default_pl.HSetNx(key, fields[0], time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("hsetNx:%v", ok)

	isNew, err := default_pl.HSet(key, fields[0], time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hset:%v", isNew)

	suc, err := default_pl.HMSet(key, fields[1], time.Now().UnixNano(), fields[2], time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hmset:%v", suc)

	suc, err = default_pl.HMSetMap(key, map[string]interface{}{
		fields[3]: time.Now().UnixNano(),
		fields[4]: time.Now().Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hmsetmap:%v", suc)

	values, err := default_pl.HMGet(key, fields[0:5]...)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hmget:%v", values)

	kv, err := default_pl.HMGetMap(key, fields[0:5]...)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("hmgetmap:%v", kv)

	kv, err = default_pl.HGetAll(key)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("hgetall:%v", kv)
}

func TestRedis_ConfigSet(t *testing.T) {
	conf, err := default_pl.ConfigGet("*")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("conf timeout:%v", conf["timeout"])

	ok, err := default_pl.ConfigSet("timeout", 0)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("conf set:%v", ok)
}

func TestRedis_ClientList(t *testing.T) {
	list, err := default_pl.ClientList()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("client list:%v", list)
}

func TestRedis_LIndex(t *testing.T) {
	pl, err := g.Get(`test_list`)
	if err != nil {
		t.Fatal(err)
	}

	var key = `test_list`

	ok, err := pl.LTrim(key, -1, -1)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("ok:%v", ok)

	item, err := pl.LPop(key)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("lpop item:%s", item)

	items, err := pl.LRange(key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("lrange :%v", items)

	length, err := pl.LPush(key, 10, 1234, []byte(`test item`), 45.67)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("push length:%d", length)

	items, err = pl.LRange(key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("lrange :%v", items)

	item, err = pl.RPop(key)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("rpop item:%s", item)

	listLength, err := pl.LLen(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("llen length:%d", listLength)
}

func TestRedis_SScan(t *testing.T) {
	pl, err := g.Get(`test_list`)
	if err != nil {
		t.Fatal(err)
	}

	var key = `test_set`

	count, err := pl.SCard(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("scard count:%d", count)

	members, err := pl.SMembers(key)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("smembers:%v", members)

	var cursor int
	cursor, members, err = pl.SScan(key, cursor, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("newCursor:%d, members:%v", cursor, members)

	addNum, err := pl.SAdd(key, time.Now().UnixNano(), time.Now().Hour())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("addNum:%d", addNum)

	members, err = pl.SInter(key, key+"d")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("inter list:%v", members)

	suc, err := pl.SMove(key, key+"d", time.Now().Hour())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("move suc:%v", suc)

	members, err = pl.SInter(key, key+"d")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("inter list:%v", members)

	members, err = pl.SUnion(key, key+"d")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("union list:%v", members)

	cursor, members, err = pl.SScan(key, cursor, "1*", 20)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("sscan newCursor:%d members:%v", cursor, members)
}

func TestRedis_ZScan(t *testing.T) {
	pl, err := g.Get(`test_list`)
	if err != nil {
		t.Fatal(err)
	}

	var (
		key    = `test_zset`
		cursor = 0
	)

	cursor, members, err := pl.ZScan(key, cursor, "", 0)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zscan newCursor:%d members:%v", cursor, members)

	count, err := pl.ZCard(key)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zcard:%d", count)

	createNum, err := pl.ZAdd(key, 102.23, time.Now().UnixNano(), 101.12, time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zadd createNum:%d", createNum)

	createNum, err = pl.ZAddMap(key, map[string]interface{}{
		"m1": 145.34,
		"m2": 101,
		"m3": 3,
		"m4": 234,
		"m5": 34234.342,
		"m6": 1000,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zaddmap createNum:%d", createNum)

	count, err = pl.ZCard(key)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zcard:%d", count)

	members, err = pl.ZRange(key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zrange:%v", members)

	members, err = pl.ZRevRange(key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zrevrange:%v", members)

	rankIndex, err := pl.ZRank(key, "m2")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zrank index:%d", rankIndex)

	score, err := pl.ZScore(key, "m3")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zscore score:%s", score)

	rankIndex, err = pl.ZRevRank(key, "m2")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zrevrank index:%d", rankIndex)

	count, err = pl.ZCount(key, "(102.23", "234")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("zcount count:%d", count)

	newScore, err := pl.ZIncrBy(key, 1.2, "m3")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("ZIncrBy newScore:%s", newScore)

	mem, err := pl.ZRevRangeByScoreWithScore(key, 234, "102.23", 0, 10)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("ZRevRangeByScoreWithScore mem:%v", mem)

	g.Range(func(index int, p Pool, hitCount uint64) (handled bool) {
		t.Logf("index:%d, hashCode:%d, hitCount:%d", index, p.HashCode(), hitCount)
		return
	})
}

func TestRedis_Multi(t *testing.T) {
	pl, err := g.Get(`test_multi`)
	if err != nil {
		t.Fatal(err)
	}

	var key = `test_multi`

	m := PipeMulti()
	if err != nil {
		t.Fatal(err)
	}

	m.Set(key, 5)
	m.Incr(key).IncrBy(key, 34)

	values, err := pl.Exec(m)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%v", values)

	m = PipeMulti()

	key = `test_multi_pipe`
	m.HGet(key, `date`).HSet(key, `date`, time.Now().Unix())
	m.HMSetMap(key, map[string]interface{}{
		"name": "nn",
		"age":  34,
	})
	m.HGetAll(key)

	values, err = pl.Exec(m)
	if err != nil {
		t.Fatal(err)
	}

	if len(values) == 4 {
		t.Log(String(values[0], nil))
		t.Log(values[1].(int64))
		t.Log(String(values[2], nil))
		t.Log(redigo.StringMap(values[3], nil))
	}
}

func TestRedis_GeoRadiusByMemberWithDist(t *testing.T) {
	pl, err := g.Get(`test_geo`)
	if err != nil {
		t.Fatal(err)
	}

	type Loc struct {
		id   int64
		addr string
		lng  float64
		lat  float64
	}

	var (
		key         = `test_geo`
		addressList = []Loc{
			{0, `北京市朝阳区酒仙桥路6号`, 116.49089, 39.982661},
			{1, `阿里中心·望京B座`, 116.489874, 40.002424},
			{2, `百度大厦`, 116.301444, 40.050923},
			{3, `腾讯北京总部大楼`, 116.273514, 40.040417},
			{4, `天津站`, 117.209954, 39.136507},
			{5, `天津南站`, 117.061157, 39.057157},
			{6, `天津西站`, 117.163322, 39.158351},
			{7, `北京站`, 116.427048, 39.902802},
			{8, `北京南站`, 116.379007, 39.865011},
			{9, `北京西站`, 116.321592, 39.894793},
			{10, `北京朝阳站`, 116.507718, 39.944463},
		}
	)

	locList, err := pl.GeoRadius(key, 0, 0, 200, "m", 10, "DESC")
	if err != nil {
		t.Fatal(err)
	}

	for _, loc := range locList {
		t.Logf("mem:%s distance:%s hash:%d lat:%f lng:%f \n", loc.Member, loc.Distance, loc.Hash, loc.Lat, loc.Lng)
		index, _ := strconv.Atoi(loc.Member)
		addr := addressList[index]
		t.Logf("id:%d addr:%s lat:%f lng:%f \n", addr.id, addr.addr, addr.lat, addr.lng)
	}

	locList, err = pl.GeoRadius(key, 0, 0, 1, "km", 0, "")
	if err != nil {
		t.Fatal(err)
	}

	for _, loc := range locList {
		t.Logf("mem:%s distance:%s hash:%d lat:%f lng:%f \n", loc.Member, loc.Distance, loc.Hash, loc.Lat, loc.Lng)
		index, _ := strconv.Atoi(loc.Member)
		addr := addressList[index]
		t.Logf("id:%d addr:%s lat:%f lng:%f \n", addr.id, addr.addr, addr.lat, addr.lng)
	}

	locList, err = pl.GeoRadiusByMember(key, 1, 100, "km", 0, "")
	if err != nil {
		t.Fatal(err)
	}

	for _, loc := range locList {
		t.Logf("mem:%s distance:%s hash:%d lat:%f lng:%f \n", loc.Member, loc.Distance, loc.Hash, loc.Lat, loc.Lng)
		index, _ := strconv.Atoi(loc.Member)
		addr := addressList[index]
		t.Logf("id:%d addr:%s lat:%f lng:%f \n", addr.id, addr.addr, addr.lat, addr.lng)
	}

	args := make([]interface{}, 0, len(addressList)*3-3)
	for start := 1; start < len(addressList); start++ {
		args = append(args, addressList[start].lng, addressList[start].lat, addressList[start].id)
	}
	createNum, err := pl.GeoAdd(key, addressList[0].lng, addressList[0].lat, addressList[0].id, args...)

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("createNum:%d", createNum)

	locList, err = pl.GeoRadiusByMember(key, addressList[1].id, 1, "km", 0, "")
	if err != nil {
		t.Fatal(err)
	}

	for _, loc := range locList {
		t.Logf("mem:%s distance:%s hash:%d lat:%f lng:%f \n", loc.Member, loc.Distance, loc.Hash, loc.Lat, loc.Lng)
		index, _ := strconv.Atoi(loc.Member)
		addr := addressList[index]
		t.Logf("id:%d addr:%s lat:%f lng:%f \n", addr.id, addr.addr, addr.lat, addr.lng)
	}

	locList, err = pl.GeoRadius(key, addressList[4].lng, addressList[4].lat, 100, "km", 5, "ASC")
	if err != nil {
		t.Fatal(err)
	}

	for _, loc := range locList {
		t.Logf("mem:%s distance:%s hash:%d lat:%f lng:%f \n", loc.Member, loc.Distance, loc.Hash, loc.Lat, loc.Lng)
		index, _ := strconv.Atoi(loc.Member)
		addr := addressList[index]
		hashList, err := pl.GeoHash(key, addr.id)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("id:%d addr:%s geohash:%s lat:%f lng:%f \n", addr.id, addr.addr, hashList[0], addr.lat, addr.lng)
	}
}

func TestRedis_GeoRadiusByMember(t *testing.T) {
	pl, err := g.Get(`test_multi`)
	if err != nil {
		t.Fatal(err)
	}

	var key = `test_geo`

	m := PipeMulti()
	if err != nil {
		t.Fatal(err)
	}

	m.GeoDist(key, 1, 2, "km")
	m.GeoDel(key, 10)
	m.GeoPos(key, 3, 4)
	m.GeoRadiusByMember(key, 0, 100, "km", 10, "")

	values, err := pl.Exec(m)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(String(values[0], nil))
	t.Log(redigo.Int(values[1], nil))

	positionList, err := Positions(values[2], nil)
	t.Logf("%#v", positionList)

	locationList, err := Locations(values[3], nil)
	t.Logf("%#v", locationList)
}

func TestRedis_Acquire(t *testing.T) {
	pl, err := g.Get(`test_lock`)
	if err != nil {
		t.Fatal(err)
	}

	var (
		key = `test_lock`
	)

	token, err := pl.Acquire(key, 3)
	if err != nil {
		t.Fatal(err)
	}

	if token == 0 {
		t.Fatal("want >0, got 0")
	}

	t.Logf("got token:%d", token)

	failToken, err := pl.Acquire(key, 3)
	if err != nil {
		t.Fatal(err)
	}

	if failToken > 0 {
		t.Fatalf("want 0, got %d", failToken)
	}

	ok, err := pl.Release(key, token)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("want true, got %v", ok)
	}

	token, err = pl.Acquire(key, 3)
	if err != nil {
		t.Fatal(err)
	}

	if token == 0 {
		t.Fatal("want >0, got 0")
	}
	t.Logf("got token:%d", token)
}

func TestRedis_CacheGet(t *testing.T) {
	pl, err := g.Get(`test_cache`)
	if err != nil {
		t.Fatal(err)
	}

	var (
		item Item
		key  = `test_cache`
	)

	item, err = pl.CacheGet(key, 10, func() []byte {
		time.Sleep(time.Second * 2)
		return []byte(time.Now().String())
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("item:%+v value:%s \n", item, base.Bytes2String(item.Value))

	item, err = pl.CacheGet(key, 10, func() []byte {
		time.Sleep(time.Second)
		return []byte(time.Now().String())
	})

	if err != nil {
		t.Fatal(err)
	}

	if !item.Hit {
		t.Fatalf("want true, got %v\n", item.Hit)
	}
	t.Logf("item:%+v value:%s \n", item, base.Bytes2String(item.Value))

	ok, err := pl.CacheRemove(key)
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatalf("want true, got %v\n", ok)
	}

	item, err = pl.CacheGet(key, 10, func() []byte {
		time.Sleep(time.Second)
		return []byte(time.Now().String())
	})

	if err != nil {
		t.Fatal(err)
	}

	if item.Hit {
		t.Fatalf("want false, got %v\n", item.Hit)
	}

	t.Logf("item:%+v value:%s \n", item, base.Bytes2String(item.Value))
}

func BenchmarkRedis_CacheGet(b *testing.B) {
	pl, err := g.Get(`test_cache`)
	if err != nil {
		b.Fatal(err)
	}

	var (
		key = `test_cache`
	)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			item, err := pl.CacheGet(key, 10, func() []byte {
				time.Sleep(time.Second)
				return []byte(time.Now().String())
			})

			if err != nil {
				b.Fatal(err)
			}

			if !item.Hit {
				b.Logf("not hit")
			}
		}
	})
}
