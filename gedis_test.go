package gedis

import (
	"github.com/grpc-boot/base"
	"log"
	"math/rand"
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

	p           Pool
	g           Group
	groupOption GroupOption
)

type GroupOption struct {
	Group []Option `yaml:"redis" json:"redis"`
}

func init() {
	p = NewPool(option)
	err := base.YamlDecodeFile("./app.yml", &groupOption)
	if err != nil {
		log.Fatal(err)
	}
}

func TestGroup_Range(t *testing.T) {
	var err error
	g, err = NewGroup(groupOption.Group...)
	if err != nil {
		t.Fatal(err)
	}

	g.Range(func(index int, p Pool, hitCount uint64) (handled bool) {
		t.Logf("index:%d, hashCode:%d, hitCount:%d", index, p.HashCode(), hitCount)
		return
	})
	t.Fatal()
}

func TestRedis_Scan(t *testing.T) {
	r := p.Get()
	defer p.Put(r)

	newCursor, v, err := r.Scan(0, "*s*", 10)
	t.Log(newCursor, v, err)
}

func TestRedis_Dump(t *testing.T) {
	r := p.Get()
	defer p.Put(r)
	val, err := r.Dump(`tests`)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(val)
}

func TestRedis_Keys(t *testing.T) {
	r := p.Get()
	defer p.Put(r)
	keys, err := r.Keys("*")
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(keys)
}

func TestRedis_RandomKey(t *testing.T) {
	r := p.Get()
	defer p.Put(r)
	key, err := r.RandomKey()
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(key)
}

func TestRedis_Type(t *testing.T) {
	r := p.Get()
	defer p.Put(r)

	key, err := r.Type(`test`)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Log(key)
}

func TestRedis_Get(t *testing.T) {
	r := p.Get()
	defer p.Put(r)

	ok, err := r.Set(`test`, time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Fatal("want true, got false")
	}

	val, err := r.Get(`test`)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(val)
}

func TestRedis_IncrByFloat(t *testing.T) {
	r := p.Get()
	defer p.Put(r)

	val, err := r.Incr(`incr-test`)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("incr test:%d", val)

	val, err = r.IncrBy(`incr-test`, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("incr by test:%d", val)

	v, err := r.IncrByFloat(`incr-test-float`, 101.34)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("incr by test:%v", v)
}

func TestRedis_GetRange(t *testing.T) {
	var (
		r   = p.Get()
		key = `test_range`
	)
	defer p.Put(r)

	val, err := r.GetRange(key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("get range:%s", val)

	length, err := r.SetRange(key, 15, time.Now().String())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("after setRange length:%d", length)

	val, err = r.GetRange(key, 15, 20)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("get range [15-20]:%s", val)
}

func TestRedis_BitCount(t *testing.T) {
	r := p.Get()
	defer p.Put(r)

	var (
		key = `test_bit`
	)

	v, err := r.GetBit(key, 1024)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(v)

	num, err := r.BitCount(key)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(num)

	ok, err := r.SetBit(key, 1024, 1)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(ok)

	num, err = r.BitCount(key)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(num)

	num, err = r.BitCount(key, 256, 512)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(num)
}

func TestRedis_MGet(t *testing.T) {
	r := p.Get()
	defer p.Put(r)

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

	values, err := r.MGet(keys...)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("mget values:%v", values)

	_, err = r.SetEx(keys[0], 60, time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}

	ok, err := r.SetNx(keys[3], time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("set nx:%v", ok)

	kv, err := r.MGetMap(keys...)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("mgetmap:%v", kv)

	suc, err := r.MSetByMap(map[string]interface{}{
		keys[2]: time.Now().UnixNano(),
		keys[5]: rand.Int63n(time.Now().Unix()),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("msetmap:%v", suc)

	values, err = r.MGet(keys...)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("mget:%+v", values)

	suc, err = r.MSet(keys[6], time.Now().UnixNano(), keys[7], time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("mset:%v", suc)
}

func TestRedis_HGetAll(t *testing.T) {
	r := p.Get()
	defer p.Put(r)

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

	ok, err := r.HSetNx(key, fields[0], time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("hsetNx:%v", ok)

	isNew, err := r.HSet(key, fields[0], time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hset:%v", isNew)

	suc, err := r.HMSet(key, fields[1], time.Now().UnixNano(), fields[2], time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hmset:%v", suc)

	suc, err = r.HMSetMap(key, map[string]interface{}{
		fields[3]: time.Now().UnixNano(),
		fields[4]: time.Now().Unix(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hmsetmap:%v", suc)

	values, err := r.HMGet(key, fields[0:5]...)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hmget:%v", values)

	kv, err := r.HMGetMap(key, fields[0:5]...)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("hmgetmap:%v", kv)

	kv, err = r.HGetAll(key)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("hgetall:%v", kv)
}

func TestRedis_ConfigSet(t *testing.T) {
	r := p.Get()
	defer p.Put(r)

	conf, err := r.ConfigGet("*")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("conf timeout:%v", conf["timeout"])

	ok, err := r.ConfigSet("timeout", 60)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("conf set:%v", ok)
}

func TestRedis_ClientList(t *testing.T) {
	r := p.Get()
	defer p.Put(r)

	list, err := r.ClientList()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("client list:%v", list)
}
