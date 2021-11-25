package main

import (
	"log"
	"sync"

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
	var key = `t_limit:127.0.0.1`
	var wa sync.WaitGroup
	wa.Add(1)

	//令牌桶限速，每秒3个请求
	work(4, func() {
		ok, err := pl.SecondLimitByToken(key, 3, 1)
		if err != nil {
			log.Fatal(err)
		}
		if ok {
			log.Println("ok")
		}
	})

	//自然时间限速，每秒3个请求
	/*work(8, func() {
		ok, err := pl.SecondLimitByTime(key, 3, 1)
		if err != nil {
			log.Fatal(err)
		}
		if ok {
			log.Println("ok")
		}
	})*/

	//自然时间限速，每分钟3个请求
	/*work(8, func() {
		ok, err := pl.MinuteLimitByTime(key, 3, 1)
		if err != nil {
			log.Fatal(err)
		}
		if ok {
			log.Println("ok")
		}
	})*/

	wa.Wait()
}

func work(num int, hand func()) {
	for i := 0; i < num; i++ {
		go func() {
			for {
				hand()
			}
		}()
	}
}
