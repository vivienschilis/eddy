package main

import (
	"github.com/garyburd/redigo/redis"
)

type RedisBroker struct {
	addr  string
	state int
}

func NewRedisBroker(addr string) *RedisBroker {
	return &RedisBroker{addr, REDIS_DISCONNECTED}
}

func (self *RedisBroker) Publish(event *Event) (err error) {
	c, err := self.connect()
	if err != nil {
		return
	}
	defer c.Close()

	b, err := DumpEvent(event)
	if err != nil {
		return
	}
	data := []byte(b)

	c.Send("MULTI")
	c.Send("ZADD", event.Channel, -1*event.Id, data)
	c.Send("ZREMRANGEBYRANK", event.Channel, BUF_SIZE, -1)
	c.Send("EXPIRE", event.Channel, BUF_EXPIRE)
	c.Send("PUBLISH", event.Channel, data)
	_, err = c.Do("EXEC")

	return
}

func (self *RedisBroker) connect() (redis.Conn, error) {
	return redis.Dial("tcp", self.addr)
}
