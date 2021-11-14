package gedis

import (
	"testing"
	"time"
)

var (
	option = Option{
		Host:            "127.0.0.1",
		Port:            "6379",
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

	p Pool
)

func init() {
	p = NewPool(option)
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
