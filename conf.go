package gedis

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/grpc-boot/base"
	"github.com/grpc-boot/base/core/zaplogger"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/atomic"
)

const (
	DefaultNotifyPattern = `KA$`
)

const (
	eventSet     = `set`
	eventDel     = `del`
	eventExpired = `expired`
)

const (
	channelPrefix = `__keyspace@0__:`
	channelFormat = channelPrefix + `%s*`
)

const (
	defaultIntervalSecond = 600
	defaultSyncPageSize   = 128
)

type ConfOption struct {
	Option             Option   `json:"option" yaml:"option"`
	Prefix             string   `json:"prefix" yaml:"prefix"`
	KeyList            []string `json:"key_list" yaml:"key_list"`
	SyncIntervalSecond int64    `json:"sync_interval_second" yaml:"sync_interval_second"`
	SyncPageSize       int      `json:"sync_page_size" yaml:"sync_page_size"`
}

func (co *ConfOption) buildKeys(keys ...string) []string {
	if len(keys) == 0 {
		return nil
	}

	redisKeys := make([]string, len(keys))
	for index, key := range keys {
		redisKeys[index] = co.Prefix + key
	}

	return redisKeys
}

type Conf struct {
	option    ConfOption
	red       Pool
	sub       SubConn
	cache     sync.Map
	syncCount atomic.Int64
}

func NewConf(option ConfOption) (c *Conf, err error) {
	c = &Conf{
		option: option,
	}

	err = c.init()
	if err != nil {
		return nil, err
	}

	return c, err
}

func (c *Conf) init() (err error) {
	c.sub, err = NewSubConn(c.option.Option)
	if err != nil {
		return
	}

	c.red = NewPool(c.option.Option)

	var intervalSecond int64 = defaultIntervalSecond
	if c.option.SyncIntervalSecond > 0 {
		intervalSecond = c.option.SyncIntervalSecond
	}

	err = c.sync(c.option.SyncPageSize)
	if err != nil {
		return err
	}

	go func() {
		er := recover()
		if er != nil {
			Error("sync data from redis panic",
				zaplogger.Error(er.(error)),
			)
		}

		ticker := time.NewTicker(time.Second * time.Duration(intervalSecond))
		for range ticker.C {
			_ = c.sync(c.option.SyncPageSize)
		}
	}()

	return c.watch(context.Background())
}

func (c *Conf) SetKeyspaceNotify(pattern string) error {
	_, err := c.red.Do("config", "set", "notify-keyspace-events", pattern)
	return err
}

func (c *Conf) Get(key string) base.JsonParam {
	value, _ := c.cache.Load(key)
	val, _ := value.(base.JsonParam)
	return val
}

func (c *Conf) Range(f func(key string, value base.JsonParam) bool) {
	c.cache.Range(func(key, value interface{}) bool {
		return f(key.(string), value.(base.JsonParam))
	})
}

func (c *Conf) sync(size int) error {
	if len(c.option.KeyList) == 0 {
		return ErrKeyList
	}

	syncAt := time.Now()
	Debug("start sync config from redis",
		zaplogger.String("Prefix", c.option.Prefix),
	)

	if size < 1 {
		size = defaultSyncPageSize
	}

	var (
		start = 0
		end   = 0
	)

	for end < len(c.option.KeyList) {
		end = start + size
		if end >= len(c.option.KeyList) {
			end = len(c.option.KeyList)
		}

		redKeys := c.option.buildKeys(c.option.KeyList[start:end]...)
		values, err := c.red.MGetBytesMap(redKeys...)
		if err != nil {
			return err
		}

		for key, value := range values {
			if err = c.set(key, value); err != nil {
				Error("set key failed",
					zaplogger.Key(key),
					zaplogger.Value(base.Bytes2String(value)),
					zaplogger.Error(err),
				)
			}
		}

		start = end
	}

	c.syncCount.Inc()

	Debug("sync config from redis done",
		zaplogger.String("Prefix", c.option.Prefix),
		zaplogger.Duration(time.Since(syncAt)),
		zaplogger.Int64("Count", c.syncCount.Load()),
	)

	return nil
}

func (c *Conf) watch(ctx context.Context) error {
	channelPattern := fmt.Sprintf(channelFormat, c.option.Prefix)
	ch, err := c.sub.PSubscribeChannel(ctx, 32, channelPattern)
	if err != nil {
		return err
	}

	go func() {
		runtime.LockOSThread()

		for {
			msg, ok := <-ch

			switch val := msg.(type) {
			case Msg:
				c.dealCmd(val)
			case Subscription:
				Debug("subscribe channel success",
					zaplogger.String("Channel", channelPattern),
				)
			case Pong:
				continue
			default:
				Debug("got msg",
					zaplogger.Any("ConfMsg", msg),
				)
			}

			if !ok {
				break
			}
		}
	}()

	return nil
}

func (c *Conf) dealCmd(msg Msg) {
	if !strings.HasPrefix(msg.Channel, channelPrefix) {
		return
	}

	redKey := msg.Channel[len(channelPrefix):]

	switch base.Bytes2String(msg.Data) {
	case eventDel, eventExpired:
		_ = c.del(redKey)
	case eventSet:
		val, err := c.red.GetBytes(redKey)
		if err != nil {
			Error("update data failed",
				zaplogger.Error(err),
				zaplogger.Key(redKey),
			)
			return
		}
		_ = c.set(redKey, val)
	default:
	}
}

func (c *Conf) set(fullKey string, value []byte) error {
	if !strings.HasPrefix(fullKey, c.option.Prefix) {
		return ErrKeyFormat
	}

	val := base.JsonParam{}
	if err := jsoniter.Unmarshal(value, &val); err != nil {
		return err
	}

	c.cache.Store(fullKey[len(c.option.Prefix):], val)
	return nil
}

func (c *Conf) del(fullKey string) error {
	if !strings.HasPrefix(fullKey, c.option.Prefix) {
		return ErrKeyFormat
	}

	c.cache.Delete(fullKey[len(c.option.Prefix):])
	return nil
}
