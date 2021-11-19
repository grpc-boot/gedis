# Redis

> redis 4 golang base on redigo

### 1. use pool

```go
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
		MaxConnLifetime: 600,  //单位秒
		MaxIdle:         10,
		MaxActive:       20,
		ReadTimeout:     300, //单位毫秒
		WriteTimeout:    0, //单位毫秒
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
```

### 2. use group

```go
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
					Host:            "127.0.0.1",
					Port:            6379,
					Auth:            "",
					Db:              0,
					MaxConnLifetime: 600, //单位秒
					MaxIdle:         10,
					MaxActive:       20,
					ReadTimeout:     300, //单位毫秒
					WriteTimeout:    0,   //单位毫秒
				},
				VirtualCount: 32,  //虚拟节点数量
			},
	}

	group, err := gedis.NewGroup(options...)
	if err != nil {
		log.Printf("init redis group err:%s", err)
	}

	var (
		userId = `123456`
		key    = `user:`+ userId
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
```

### 3. pub/sub

> app.yml

```yaml
host: '127.0.0.1'
port: 6379
```

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/grpc-boot/gedis"

	"github.com/grpc-boot/base"
)

var (
	subConn gedis.SubConn
	option  gedis.Option
)

func init() {
	err := base.YamlDecodeFile("./app.yml", &option)
	if err != nil {
		base.RedFatal("load config err:%s", err.Error())
	}

	subConn, err = gedis.NewSubConn(option)
	if err != nil {
		base.RedFatal("new sub conn err:%s", err.Error())
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-time.After(time.Second * 10)
		cancel()
	}()

	sub(ctx)
	publish()
}

func sub(ctx context.Context) {
	ch, err := subConn.PSubscribeChannel(ctx, 1024, `chan*`)
	if err != nil {
		base.RedFatal("subscribe err:%s", err.Error())
	}

	for w := 0; w < 8; w++ {
		go func() {
			for {
				msg, isOpen := <-ch
				if !isOpen {
					return
				}

				switch m := msg.(type) {
				case gedis.Msg:
					base.Green("receive pattern:%s channel:%s, msg:%s", m.Pattern, m.Channel, string(m.Data))
				case error:
					base.Red("receive err:%s", m.Error())
				case gedis.Pong:
					continue
				}

			}
		}()
	}
}

func publish() {
	var (
		tick   = time.NewTicker(time.Second)
		num    = 0
		recNum int
		err    error
	)

	for range tick.C {
		num++

		ch, msg := fmt.Sprintf("chan%d", num%2+1), fmt.Sprintf(`{"id":%d, "cmd":"chat", "data":{}}`, num)
		p := gedis.NewPool(option)
		recNum, err = p.Publish(ch, msg)
		if err != nil {
			base.Red("publish err:%s", err.Error())
			continue
		}
		base.Green("publish recNum:%d, ch:%s, msg:%s", recNum, ch, msg)
	}
}
```

### 4. lock

```go
package main

import (
	"log"

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
	var key = `t_lock`

	token, err := pl.Acquire(key, 3)
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
```

### 5. cache

```go
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
```

