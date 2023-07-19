package gedis

import "sync"

var (
	_container sync.Map
)

func Set(key string, value interface{}) {
	_container.Store(key, value)
}

func Get(key string) (value interface{}) {
	value, _ = _container.Load(key)
	return value
}

func SetPool(key string, option Option) {
	Set(key, NewPool(option))
}

func SetPoolByJson(key string, jsonStr string) (err error) {
	p, err := NewPoolWithJson(jsonStr)
	if err != nil {
		return err
	}

	Set(key, p)
	return err
}

func GetPool(key string) Pool {
	val, _ := Get(key).(Pool)
	return val
}

func SetGroup(key string, options ...GroupOption) (err error) {
	g, err := NewGroup(options...)
	if err != nil {
		return err
	}

	Set(key, g)

	return err
}

func GetGroup(key string) Group {
	g, _ := Get(key).(Group)
	return g
}

func Range(handler func(key string, pool Pool, group Group) bool) {
	_container.Range(func(key, value interface{}) bool {
		switch val := value.(type) {
		case Pool:
			return handler(key.(string), val, nil)
		case Group:
			return handler(key.(string), nil, val)
		default:
			return true
		}
	})
}
