package main

import (
	"math/rand"
	"time"

	"github.com/grpc-boot/gedis"

	"github.com/grpc-boot/base"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	option := gedis.Option{
		Host:                  "127.0.0.1",
		Port:                  6379,
		Auth:                  "",
		Db:                    0,
		MaxConnLifetimeSecond: 30,
		IdleTimeoutSecond:     10,
		MaxActive:             1,
		Wait:                  false,
	}

	conf, err := gedis.NewConf(gedis.ConfOption{
		Option: option,
		Prefix: "conf_",
		KeyList: []string{
			"control",
			"news",
			"days",
			"datetime",
			"cloud",
		},
		SyncIntervalSecond: 3,
		SyncPageSize:       2,
	})

	if err != nil {
		base.RedFatal("init config failed with error:%s", err.Error())
	}

	err = conf.SetKeyspaceNotify(gedis.DefaultNotifyPattern)
	if err != nil {
		base.RedFatal("set keyspace notify failed with error:%s", err.Error())
	}

	go func() {
		red := gedis.NewPool(option)
		writeTicker := time.NewTicker(time.Second)
		for range writeTicker.C {
			switch rand.Intn(6) {
			case 0:
				data := base.JsonParam{
					"startAt": time.Now().Unix(),
					"Rate":    rand.Intn(100),
					"Name":    "配置检测",
				}

				if _, err = red.Set("conf_control", data.JsonMarshal()); err != nil {
					base.Red("set failed with error: %s", err.Error())
				}
			case 1:
				if _, err = red.Del("conf_control"); err != nil {
					base.Red("del failed with error: %s", err.Error())
				}
			case 2:
				if _, err = red.Expire("conf_control", 2); err != nil {
					base.Red("set expire failed with error: %s", err.Error())
				}
			case 3:
				data := base.JsonParam{
					"startAt": time.Now().Unix(),
					"Rate":    rand.Intn(100),
					"Name":    "云控",
				}
				if _, err = red.SetEx("conf_cloud", 2, data.JsonMarshal()); err != nil {
					base.Red("set ex failed with error: %s", err.Error())
				}
			case 4:
				data := base.JsonParam{
					"startAt": time.Now().Unix(),
					"Rate":    rand.Intn(100),
					"Name":    "云控",
				}
				if _, err = red.SetNx("conf_cloud", data.JsonMarshal()); err != nil {
					base.Red("set nx failed with error: %s", err.Error())
				}
			case 5:
				if _, err = red.ExpireAt("conf_cloud", time.Now().Unix()+2); err != nil {
					base.Red("set expire at failed with error: %s", err.Error())
				}
			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		cloud := conf.Get("cloud")
		if cloud != nil {
			base.Green("Name:%s Rate:%d startAt:%d", cloud.String("Name"), cloud.Int("Rate"), cloud.Int64("startAt"))
		}
	}
}
