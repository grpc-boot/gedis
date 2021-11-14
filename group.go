package gedis

import (
	"context"
	"errors"

	"github.com/grpc-boot/base"
)

var (
	ErrOptionEmpty = errors.New(`redis option empty`)
)

type Group interface {
	GetPool(key string) (p Pool, err error)
	Get(key string) (redis Conn, err error)
	GetContext(ctx context.Context, key string) (redis Conn, err error)
	Put(redis Conn) (err error)
}

type group struct {
	Group

	ring base.HashRing
}

func NewGroup(options ...Option) (g Group, err error) {
	poolSize := len(options)
	if poolSize < 1 {
		return nil, ErrOptionEmpty
	}

	poolList := make([]base.CanHash, len(options), len(options))
	for index := 0; index < poolSize; index++ {
		poolList[index] = NewPool(options[index])
	}

	g = &group{
		ring: base.NewHashRing(poolList...),
	}

	return g, nil
}

func (g *group) GetPool(key string) (p Pool, err error) {
	var r base.CanHash
	r, err = g.ring.Get(key)
	if err != nil {
		return nil, err
	}

	return r.(Pool), nil
}

func (g *group) Get(key string) (redis Conn, err error) {
	r, err := g.ring.Get(key)
	if err != nil {
		return nil, err
	}

	redis = r.(Pool).Get()

	return
}

func (g *group) GetContext(ctx context.Context, key string) (redis Conn, err error) {
	r, err := g.ring.Get(key)
	if err != nil {
		return nil, err
	}

	return r.(Pool).GetContext(ctx)
}

func (g *group) Put(redis Conn) (err error) {
	return redis.Close()
}
