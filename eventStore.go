package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
)

type SubscriberConnection struct {
	id        string
	channels  []*Channel
	forwarder Forwarder
	q         chan Event
}

type PublisherConnection struct {
	redis redis.Conn
	q     chan Event
}

type Forwarder interface {
	WriteId(id int64)
	Write(msg string)
	CloseNotify() <-chan bool
}

func NewSubscriberConnection(channels []string, forwarder Forwarder) (conn *SubscriberConnection, err error) {
	conn = &SubscriberConnection{
		uuid.Generate(10),
		[]*Channel{},
		forwarder,
		make(chan Event),
	}

	for _, value := range channels {
		c, err := chanRegistry.EnterChannel(value, conn)

		if err != nil {
			return nil, err
		}

		conn.channels = append(conn.channels, c)
	}

	return conn, nil
}

func (self *SubscriberConnection) Close() error {
	for _, channel := range self.channels {
		chanRegistry.LeaveChannel(channel.name, self)
	}

	msg := fmt.Sprintf("Subscription id : %s - closed", self.id)
	fmt.Println(msg)

	return nil
}

func (self *SubscriberConnection) Listen() {
	msg := fmt.Sprintf("Subscription id : %s - connected", self.id)
	fmt.Println(msg)

	for _, channel := range self.channels {
		go channel.SendHistory(self.id)
	}

	for {
		select {
		case <-self.forwarder.CloseNotify():
			return
		case event := <-self.q:
			self.forwarder.WriteId(event.Id)
			self.forwarder.Write(event.Data)
		}
	}
}

const BUF_EXPIRE = 3600
const BUF_SIZE = 10

func (self *PublisherConnection) SendEvent(event Event) {

	// TODO: Error checking
	b, _ := json.Marshal(event)
	data := fmt.Sprintf("%s", b)

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
		make(chan Event),
	}

	go eventPublisher.run()
}
