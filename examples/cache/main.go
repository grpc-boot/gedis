package main

import (
	"log"
	"time"

	"github.com/grpc-boot/gedis"
)

func main() {
	option := gedis.Option{
		Host:            "127.0.0.1",
		Port:            6379,
		Auth:            "",
		Db:              0,
		MaxConnLifetime: 600, //单位秒
		MaxIdle:         10,
		MaxActive:       20,
		ReadTimeout:     300, //单位毫秒
		WriteTimeout:    0,   //单位毫秒
	}

	pl := gedis.NewPool(option)
	var key = `t_cache`

	item, err := pl.CacheGet(key, 60, func() []byte {
		//模拟耗时
		time.Sleep(1)
		return []byte(time.Now().String())
	})

	if err != nil {
		log.Fatalf("get cache err:%s", err.Error())
	}

	log.Printf("%+v\n", item)

	item, err = pl.CacheGet(key, 60, func() []byte {
		//模拟耗时
		time.Sleep(1)
		return []byte(time.Now().String())
	})

	if err != nil {
		log.Fatalf("get cache err:%s", err.Error())
	}

	log.Printf("%+v\n", item)
}
