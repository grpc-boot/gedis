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
	Port string `yaml:"port" json:"port"`
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
	pl := &redigo.Pool{
		MaxConnLifetime: time.Second * time.Duration(option.MaxConnLifetime),
		MaxIdle:         option.MaxIdle,
		MaxActive:       option.MaxActive,
		Wait:            option.Wait,
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp",
				fmt.Sprintf("%s:%s", option.Host, option.Port),
				redigo.DialConnectTimeout(time.Millisecond*time.Duration(option.ConnectTimeout)),
				redigo.DialReadTimeout(time.Millisecond*time.Duration(option.ReadTimeout)),
				redigo.DialReadTimeout(time.Millisecond*time.Duration(option.ReadTimeout)),
			)
			if err != nil {
				return nil, err
			}

			if len(option.Auth) > 0 {
				if _, err = c.Do("AUTH", option.Auth); err != nil {
					_ = c.Close()
					return nil, err
				}
			}

			if _, err = c.Do("SELECT", option.Db); err != nil {
				_ = c.Close()
				return nil, err
			}
			return c, nil
		},
	}

	return &pool{
		p:  pl,
		id: []byte(fmt.Sprintf("%s:%s", option.Host, option.Port)),
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
