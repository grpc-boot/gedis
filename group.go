package gedis

import (
	"errors"

	"github.com/grpc-boot/base"
)

var (
	ErrOptionEmpty = errors.New(`redis option empty`)
)

type Group interface {
	Get(key interface{}) (p Pool, err error)
	Index(index int) (p Pool, err error)
	Range(handler func(index int, p Pool, hitCount uint64) (handled bool))
}

type group struct {
	ring base.HashRing
}

// NewGroup 实例化Group
func NewGroup(options ...GroupOption) (g Group, err error) {
	if len(options) < 1 {
		return nil, ErrOptionEmpty
	}

	var poolSize = 0
	for _, option := range options {
		poolSize += 1
		poolSize += option.VirtualCount
	}

	poolList := make([]base.CanHash, 0, poolSize)
	for _, option := range options {
		for s := 0; s <= option.VirtualCount; s++ {
			option.Option.Index = s
			poolList = append(poolList, NewPool(option.Option))
		}
	}

	g = &group{
		ring: base.NewHashRing(poolList...),
	}
	return g, nil
}

// Get 根据key获取Pool
func (g *group) Get(key interface{}) (pool Pool, err error) {
	var r base.CanHash
	r, err = g.ring.Get(key)
	if err != nil {
		return nil, err
	}

	return r.(Pool), nil
}

// Index 根据索引获取Pool
func (g *group) Index(index int) (pool Pool, err error) {
	r, err := g.ring.Index(index)
	if err != nil {
		return nil, err
	}

	return r.(Pool), nil
}

// Range 遍历Pool
func (g *group) Range(handler func(index int, p Pool, hitCount uint64) (handled bool)) {
	g.ring.Range(func(index int, server base.CanHash, hitCount uint64) (handled bool) {
		return handler(index, server.(Pool), hitCount)
	})
}
