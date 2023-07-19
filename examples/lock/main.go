package main

import (
	"log"

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

	pl := gedis.NewPool(option)
	var key = `t_lock`

	token, err := pl.Acquire(key, 10)
	if err != nil {
		log.Fatalf("get lock err:%s", err.Error())
	}

	if token == 0 {
		log.Println("未获得锁")
		log.Println("逻辑处理")
		return
	}

	//释放锁
	defer pl.Release(key, token)

	log.Println("获得锁")
	log.Println("正常逻辑")
}
