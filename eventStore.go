package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"sync"
	"time"
)

var chanRegistry *ChannelRegistry

type Channel struct {
	name    string
	psc     redis.PubSubConn
	c       chan Event
	closing chan bool
	wg      sync.WaitGroup
}

type SubscriberConnection struct {
	id        string
	channels  []*Channel
	forwarder Forwarder
}

type PublisherConnection struct {
	redis redis.Conn
	c     chan Event
}

type Forwarder interface {
	Write(msg string)
	CloseNotify() <-chan bool
}

type Event struct {
	Channel string
	Data    string
}

func (self *Channel) SendHistory(q chan Event) {
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		return
	}
	defer conn.Close()

	n, err := redis.Values(conn.Do("ZRANGE", self.name, 0, -1))
	if err != nil {
		return
	}

	for _, x := range n {
		q <- Event{self.name, string(x.([]byte))}
	}
}

func (self *Channel) Read() (q chan string) {
	q = make(chan string)

	go func() {
		for {
			switch n := self.psc.Receive().(type) {
			case redis.Message:
				q <- string(n.Data)
				return
			case error:
				<-time.After(2 * time.Second)
				fmt.Printf("Error:", n)
				return
			}
		}
	}()

	return q
}

func (self *Channel) run() {
	defer self.psc.Close()

	if err := self.psc.Subscribe(self.name); err != nil {
		return
	}

	for {
		select {
		case <-self.closing:
			fmt.Println("Channel", self.name, "Closed")
			return
		case msg := <-self.Read():
			self.c <- Event{self.name, msg}
		}
	}

}

func (self *Channel) Close() {
	self.closing <- true
}

func NewSubscriberConnection(channels []string, forwarder Forwarder) (conn *SubscriberConnection, err error) {
	conn = &SubscriberConnection{
		uuid.Generate(10),
		[]*Channel{},
		forwarder,
	}

	for _, value := range channels {
		c, err := chanRegistry.EnterChannel(value)

		if err != nil {
			return nil, err
		}

		conn.channels = append(conn.channels, c)
	}

	return conn, nil
}

func (self *SubscriberConnection) Close() error {
	for _, channel := range self.channels {
		chanRegistry.LeaveChannel(channel.name)
	}

	msg := fmt.Sprintf("Subscription id : %s - closed", self.id)
	fmt.Println(msg)

	return nil
}

func (self *SubscriberConnection) Read(q chan Event) chan Event {
	for _, channel := range self.channels {
		go func(channel *Channel) {
			event := <-channel.c
			q <- event
		}(channel)
	}

	return q
}

func (self *SubscriberConnection) Listen() {
	msg := fmt.Sprintf("Subscription id : %s - connected", self.id)
	fmt.Println(msg)

	q := make(chan Event)
	for _, channel := range self.channels {
		fmt.Println("History")
		go channel.SendHistory(q)
	}

	for {
		select {
		case <-self.forwarder.CloseNotify():
			return
		case event := <-self.Read(q):
			msg := fmt.Sprintf("Message: %s %s\n", event.Channel, event.Data)
			self.forwarder.Write(msg)
		}
	}
}

const BUF_EXPIRE = 3600
const BUF_SIZE = 10

func (self *PublisherConnection) SendEvent(event Event) {
	timestamp := time.Now().Unix()

	// TODO: Error checking
	self.redis.Do("ZADD", event.Channel, timestamp, event.Data)
	self.redis.Do("ZREMRANGEBYRANK", event.Channel, BUF_SIZE, -1)
	self.redis.Do("EXPIRE", event.Channel, BUF_EXPIRE)
	self.redis.Do("PUBLISH", event.Channel, event.Data)
}

func (self *PublisherConnection) Close() error {
	log.Println("Publisher Close")
	return self.redis.Close()
}

func (self *PublisherConnection) run() {
	for {
		event := <-self.c
		self.SendEvent(event)
	}
}

var eventPublisher *PublisherConnection

type ChannelRegistry struct {
	dict map[string]*Channel
}

func (self *ChannelRegistry) LeaveChannel(name string) {
	if channel, ok := self.dict[name]; ok {
		channel.wg.Done()
	}
}

func (self *ChannelRegistry) EnterChannel(name string) (channel *Channel, err error) {
	if channel, ok := self.dict[name]; ok {
		fmt.Println("Channel", name, "exists")

		channel.wg.Add(1)
		return channel, nil
	}

	channel, err = self.NewChannel(name)
	fmt.Println("Open new Channel \"", channel.name, "\"")

	if err != nil {
		return nil, err
	}

	channel.wg.Add(1)
	self.dict[name] = channel

	go channel.run()

	go func(c *Channel) {
		c.wg.Wait()
		delete(self.dict, c.name)
		fmt.Println("Close Channel \"", c.name, "\"")
		c.Close()
	}(channel)

	return channel, nil
}

func (self *ChannelRegistry) NewChannel(name string) (eventChannel *Channel, err error) {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		return
	}

	eventChannel = &Channel{
		name:    name,
		psc:     redis.PubSubConn{Conn: c},
		c:       make(chan Event),
		closing: make(chan bool),
	}

	return
}

func init() {
	chanRegistry = &ChannelRegistry{
		make(map[string]*Channel),
	}

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
