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
