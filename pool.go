package gedis

import (
	"context"
	"fmt"
	"hash/crc32"
	"time"

	redigo "github.com/garyburd/redigo/redis"
	"github.com/grpc-boot/base"
)

type Option struct {
	Host string `yaml:"host" json:"host"`
	Port int    `yaml:"port" json:"port"`
	Auth string `yaml:"auth" json:"auth"`
	Db   uint8  `yaml:"db" json:"db"`
	//单位s
	MaxConnLifetime int  `yaml:"maxConnLifetime" json:"maxConnLifetime"`
	MaxIdle         int  `yaml:"maxIdle" json:"maxIdle"`
	MaxActive       int  `yaml:"maxActive" json:"maxActive"`
	Wait            bool `yaml:"wait" json:"wait"`
	//单位ms
	ConnectTimeout int `yaml:"connectTimeout" json:"connectTimeout"`
	//单位ms
	ReadTimeout int `yaml:"readTimeout" json:"readTimeout"`
	//单位ms
	WriteTimeout int `yaml:"writeTimeout" json:"writeTimeout"`
	//虚拟节点索引
	Index uint8 `yaml:"index" json:"index"`
}

type Pool interface {
	base.CanHash

	Get() (conn Conn)
	GetContext(ctx context.Context) (conn Conn, err error)
	Put(conn Conn) (err error)
	ActiveCount() (num int)
	IdleCount() (num int)
	Stats() redigo.PoolStats
	Close() (err error)
}

type pool struct {
	Pool

	p  *redigo.Pool
	id []byte
}

func NewPool(option Option) (p Pool) {
	var dialOptions = []redigo.DialOption{
		redigo.DialDatabase(int(option.Db)),
	}

	if option.ConnectTimeout > 0 {
		dialOptions = append(dialOptions, redigo.DialReadTimeout(time.Millisecond*time.Duration(option.ConnectTimeout)))
	}

	if option.ReadTimeout > 0 {
		dialOptions = append(dialOptions, redigo.DialReadTimeout(time.Millisecond*time.Duration(option.ReadTimeout)))
	}

	if option.WriteTimeout > 0 {
		dialOptions = append(dialOptions, redigo.DialWriteTimeout(time.Millisecond*time.Duration(option.WriteTimeout)))
	}

	if len(option.Auth) > 0 {
		dialOptions = append(dialOptions, redigo.DialPassword(option.Auth))
	}

	addr := fmt.Sprintf("%s:%d", option.Host, option.Port)
	pl := &redigo.Pool{
		MaxIdle:   option.MaxIdle,
		MaxActive: option.MaxActive,
		Wait:      option.Wait,
		Dial: func() (redigo.Conn, error) {
			return redigo.Dial("tcp", addr, dialOptions...)
		},
		TestOnBorrow: func(c redigo.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	if option.MaxConnLifetime > 0 {
		pl.MaxConnLifetime = time.Second * time.Duration(option.MaxConnLifetime)
	}

	return &pool{
		p:  pl,
		id: []byte(fmt.Sprintf("%s:%d-%d", option.Host, option.Port, option.Index)),
	}
}

func (p *pool) HashCode() uint32 {
	return crc32.ChecksumIEEE(p.id)
}

func (p *pool) Get() (conn Conn) {
	return newConn(p.p.Get())
}

func (p *pool) GetContext(ctx context.Context) (conn Conn, err error) {
	var r redigo.Conn
	r, err = p.p.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	return newConn(r), nil
}

func (p *pool) Put(conn Conn) (err error) {
	return conn.Close()
}

func (p *pool) ActiveCount() (num int) {
	return p.p.ActiveCount()
}

func (p *pool) IdleCount() (num int) {
	return p.p.IdleCount()
}

func (p *pool) Stats() redigo.PoolStats {
	return p.p.Stats()
}

func (p *pool) Close() (err error) {
	return p.p.Close()
}
