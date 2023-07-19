package main

import (
	"log"
	"time"

	"github.com/grpc-boot/gedis"
)

func main() {
	option := gedis.Option{
		Host:                  "127.0.0.1",
		Port:                  6379,
		Auth:                  "",
		Db:                    0,
		MaxConnLifetimeSecond: 600, //单位秒
		MaxIdle:               10,
		MaxActive:             20,
		ReadTimeout:           300, //单位毫秒
		WriteTimeout:          0,   //单位毫秒
	}

	pool := gedis.NewPool(option)
	var key = `gedis`

	ok, err := pool.Set(key, time.Now().Unix())
	if err != nil {
		log.Fatalf("set err:%s\n", err.Error())
	}

	if !ok {
		log.Printf("set failed\n")
	}

	val, _ := pool.Get(key)
	log.Printf("get val:%s\n", val)
}
