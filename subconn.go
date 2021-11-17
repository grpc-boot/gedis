package gedis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	redigo "github.com/garyburd/redigo/redis"
)

const (
	badConnFlag = `use of closed network connection`
)

var (
	retryInterval = time.Millisecond * 500
)

var (
	ErrUnKnownSubMsg = errors.New(`unknown pubsub notification`)
)

type Msg struct {
	Pattern string
	Channel string
	Data    []byte
}

type Subscription struct {
	Kind    string
	Channel string
	Count   int
}

type Pong struct {
	Data string
}

type SubConn interface {
	Close() error
	Subscribe(channels ...interface{}) error
	SubscribeChannel(ctx context.Context, size int, channels ...interface{}) (ch chan interface{}, err error)
	PSubscribe(channels ...interface{}) error
	PSubscribeChannel(ctx context.Context, size int, channels ...interface{}) (ch chan interface{}, err error)
	Unsubscribe(channels ...interface{}) error
	PUnsubscribe(channels ...interface{}) error
	Ping(data string) error
	Receive() interface{}
	ReceiveWithTimeout(timeout time.Duration) interface{}
}

type subConn struct {
	option Option
	conn   redigo.Conn
	mu     sync.Mutex
}

func NewSubConn(option Option) (SubConn, error) {
	sc := &subConn{
		option: option,
	}

	err := sc.loadConn()
	if err != nil {
		return nil, err
	}
	return sc, nil
}

func (sc *subConn) loadConn() (err error) {
	var (
		addr = fmt.Sprintf("%s:%d", sc.option.Host, sc.option.Port)
		conn redigo.Conn
	)
	conn, err = redigo.Dial("tcp", addr)

	if err != nil {
		return err
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.conn = conn
	return nil
}

func (sc *subConn) Close() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.conn.Close()
}

func (sc *subConn) Subscribe(channels ...interface{}) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	err := sc.conn.Send("SUBSCRIBE", channels...)
	if err != nil {
		return err
	}

	return sc.conn.Flush()
}

func (sc *subConn) SubscribeChannel(ctx context.Context, size int, channels ...interface{}) (ch chan interface{}, err error) {
	err = sc.Subscribe(channels...)
	if err != nil {
		return nil, err
	}

	ch = make(chan interface{}, size)

	go func() {
		tick := time.NewTicker(time.Second * 30)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				close(ch)
				_ = sc.Unsubscribe(channels...)
				return
			case <-tick.C:
				_ = sc.Ping("hc")
			default:
				switch msg := sc.Receive().(type) {
				case error:
					ch <- msg

					if strings.Contains(msg.Error(), badConnFlag) {
						if err = sc.loadConn(); err != nil {
							time.Sleep(retryInterval)
							continue
						}

						if err = sc.Subscribe(channels...); err != nil {
							time.Sleep(retryInterval)
						}
						continue
					}
				default:
					ch <- msg
				}
			}
		}
	}()
	return
}

func (sc *subConn) PSubscribe(channels ...interface{}) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	err := sc.conn.Send("PSUBSCRIBE", channels...)
	if err != nil {
		return err
	}
	return sc.conn.Flush()
}

func (sc *subConn) PSubscribeChannel(ctx context.Context, size int, channels ...interface{}) (ch chan interface{}, err error) {
	err = sc.PSubscribe(channels...)
	if err != nil {
		return nil, err
	}

	ch = make(chan interface{}, size)

	go func() {
		tick := time.NewTicker(time.Second * 30)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				close(ch)
				_ = sc.PUnsubscribe(channels...)
				return
			case <-tick.C:
				_ = sc.Ping("hc")
			default:
				switch msg := sc.Receive().(type) {
				case error:
					ch <- msg

					if strings.Contains(msg.Error(), badConnFlag) {
						if err = sc.loadConn(); err != nil {
							time.Sleep(retryInterval)
							continue
						}

						if err = sc.PSubscribe(channels...); err != nil {
							time.Sleep(retryInterval)
						}
						continue
					}
				default:
					ch <- msg
				}
			}
		}
	}()
	return
}

func (sc *subConn) Unsubscribe(channels ...interface{}) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	err := sc.conn.Send("UNSUBSCRIBE", channels...)
	if err != nil {
		return err
	}

	return sc.conn.Flush()
}

func (sc *subConn) PUnsubscribe(channels ...interface{}) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	err := sc.conn.Send("PUNSUBSCRIBE", channels...)
	if err != nil {
		return err
	}
	return sc.conn.Flush()
}

func (sc *subConn) Ping(data string) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	err := sc.conn.Send("PING", data)
	if err != nil {
		return err
	}
	return sc.conn.Flush()
}

func (sc *subConn) Receive() interface{} {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.receiveInternal(sc.conn.Receive())
}

func (sc *subConn) ReceiveWithTimeout(timeout time.Duration) interface{} {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	return sc.receiveInternal(redigo.ReceiveWithTimeout(sc.conn, timeout))
}

func (sc *subConn) receiveInternal(replyArg interface{}, errArg error) interface{} {
	reply, err := redigo.Values(replyArg, errArg)
	if err != nil {
		return err
	}

	var kind string
	reply, err = redigo.Scan(reply, &kind)
	if err != nil {
		return err
	}

	switch kind {
	case "message":
		var m Msg
		if _, err = redigo.Scan(reply, &m.Channel, &m.Data); err != nil {
			return err
		}
		return m
	case "pmessage":
		var pm Msg
		if _, err = redigo.Scan(reply, &pm.Pattern, &pm.Channel, &pm.Data); err != nil {
			return err
		}
		return pm
	case "subscribe", "psubscribe", "unsubscribe", "punsubscribe":
		s := Subscription{Kind: kind}
		if _, err = redigo.Scan(reply, &s.Channel, &s.Count); err != nil {
			return err
		}
		return s
	case "pong":
		var p Pong
		if _, err = redigo.Scan(reply, &p.Data); err != nil {
			return err
		}
		return p
	}
	return ErrUnKnownSubMsg
}
