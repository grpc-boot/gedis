package gedis

import (
	"testing"
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
	newCursor, v, err := r.Scan(0, "*s*", 10)
	t.Log(newCursor, v, err)
}
