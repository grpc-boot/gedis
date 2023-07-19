package main

import (
	"log"
	"math/rand"

	"github.com/grpc-boot/gedis"
)

func main() {
	options := []gedis.GroupOption{
		{
			Option: gedis.Option{
				Host:                  "127.0.0.1",
				Port:                  6379,
				Auth:                  "",
				Db:                    0,
				MaxConnLifetimeSecond: 600, //单位秒
				MaxIdle:               10,
				MaxActive:             20,
				ReadTimeout:           300, //单位毫秒
				WriteTimeout:          0,   //单位毫秒
			},
			VirtualCount: 32, //虚拟节点数量
		},
	}

	group, err := gedis.NewGroup(options...)
	if err != nil {
		log.Printf("init redis group err:%s", err)
	}

	var (
		userId = `123456`
		key    = `user:` + userId
	)

	pool, err := group.Get(userId)
	if err != nil {
		log.Printf("get pool err:%s", err)
	}

	ok, err := pool.HMSet(key, "name", "user1", "age", rand.Int31())
	if err != nil {
		log.Printf("get pool err:%s", err)
	}

	if !ok {
		log.Printf("hmset failed\n")
	}

	kv, err := pool.HGetAll(key)
	if err != nil {
		log.Printf("get pool err:%s", err)
	}
	log.Printf("userinfo:%+v\n", kv)
}
