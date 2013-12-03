package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
)

type PublisherConnection struct {
	redis redis.Conn
	q     chan *Event
}

const BUF_EXPIRE = 3600
const BUF_SIZE = 10

func (self *PublisherConnection) SendEvent(event *Event) {

	// TODO: Error checking
	b, _ := DumpEvent(event)
	data := fmt.Sprintf("%s", b)

	// TODO: Error checking
	self.redis.Do("ZADD", event.Channel, -1*event.Id, data)
	self.redis.Do("ZREMRANGEBYRANK", event.Channel, BUF_SIZE, -1)
	self.redis.Do("EXPIRE", event.Channel, BUF_EXPIRE)
	self.redis.Do("PUBLISH", event.Channel, data)
}

func (self *PublisherConnection) Close() error {
	log.Println("Publisher Close")
	return self.redis.Close()
}

func (self *PublisherConnection) run() {
	for {
		event := <-self.q
		self.SendEvent(event)
	}
}

var eventPublisher *PublisherConnection

func init() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		return
	}

	eventPublisher = &PublisherConnection{
		c,
		make(chan *Event),
	}

	go eventPublisher.run()
}
