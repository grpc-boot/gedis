package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grpc-boot/base"
	"github.com/grpc-boot/gedis"

	redigo "github.com/garyburd/redigo/redis"
)

var (
	g           gedis.Group
	groupOption GroupOption

	msgChan = make(chan redigo.Message, 1024)
)

type GroupOption struct {
	Group []gedis.Option `yaml:"redis" json:"redis"`
}

func init() {
	err := base.YamlDecodeFile("./app.yml", &groupOption)
	if err != nil {
		base.RedFatal("load config err:%s", err.Error())
	}

	for _, o := range groupOption.Group {
		base.Red("readTimeout:%d", o.ReadTimeout)
	}

	g, err = gedis.NewGroup(groupOption.Group...)
	if err != nil {
		base.RedFatal("create group err:%s", err.Error())
	}
}

func main() {
	var wa sync.WaitGroup
	wa.Add(1)
	go subscribe(&wa)

	for w := 0; w < 8; w++ {
		go func() {
			for {
				msg, isOpen := <-msgChan
				if !isOpen {
					break
				}
				base.Green("chanel:%s, msg:%s", msg.Channel, string(msg.Data))
			}
		}()
	}

	publish()
}

func subscribe(wa *sync.WaitGroup) {
	r, err := g.Get(`pub/sub`)
	if err != nil {
		base.RedFatal("get redis err:%s", err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer func() {
		cancel()
		g.Put(r)
	}()

	err = r.Subscribe(ctx, msgChan, `chan1`, `chan2`)
	if err != nil {
		base.RedFatal("subscribe err:%s", err.Error())
	}

	wa.Wait()
}

func publish() {
	r, err := g.Get(`pub/sub`)
	if err != nil {
		base.RedFatal("get redis err:%s", err.Error())
	}
	defer g.Put(r)

	var (
		tick   = time.NewTicker(time.Second)
		num    = 0
		recNum int
	)

	for range tick.C {
		num++

		ch, msg := fmt.Sprintf("chan%d", num%2+1), fmt.Sprintf(`{"id":%d, "cmd":"chat", "data":{}}`, num)
		recNum, err = r.Publish(ch, msg)
		if err != nil {
			base.Red("publish err:%s", err.Error())
			continue
		}
		base.Green("publish recNum:%d, ch:%s, msg:%s", recNum, ch, msg)
	}
}
