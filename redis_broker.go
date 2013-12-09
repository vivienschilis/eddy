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

// Each Publish initiates a new connection
func (self *RedisBroker) Publish(channel string, event *Event) (err error) {
	c, err := self.connect()
	if err != nil {
		return
	}
	defer c.Close()

	data := DumpEvent(event)

	c.Send("MULTI")
	c.Send("ZADD", channel, -1*event.At, data)
	c.Send("ZREMRANGEBYRANK", channel, event.Size, -1)
	c.Send("EXPIRE", channel, event.TTL)
	c.Send("PUBLISH", channel, data)
	_, err = c.Do("EXEC")

	return
}

func (self *RedisBroker) connect() (redis.Conn, error) {
	return redis.Dial("tcp", self.addr)
}
