package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"sync"
)

var chanRegistry *ChannelRegistry

type Channel struct {
	name    string
	psc     redis.PubSubConn
	listeners map[string]chan Event
	closing chan bool
	wg      sync.WaitGroup
}

type SubscriberConnection struct {
	id        string
	channels  []*Channel
	forwarder Forwarder
	q					chan Event
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

type Event struct {
	Id      int64  `json:"id"`
	Channel string `json:"name"`
	Data    string `json:"data"`
}

func (self *Channel) SendHistory(q chan Event) {
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		return
	}
	defer conn.Close()

	n, err := redis.Values(conn.Do("ZREVRANGE", self.name, 0, -1))
	if err != nil {
		return
	}

	for _, x := range n {
		var e Event
		json.Unmarshal(x.([]byte), &e)
		q <- e
	}
}

func (self *Channel) AddListener(id string, q chan Event) {
	self.listeners[id] = q
	self.wg.Add(1)
	fmt.Println("Add", self.listeners)
}

func (self *Channel) RemoveListener(id string) {
	delete(self.listeners, id)
	self.wg.Done()
	fmt.Println("Remove", self.listeners)
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
			e := Event{}
			json.Unmarshal([]byte(msg), &e)
			fmt.Println("Received data", e.Data)
			for _, listener := range self.listeners {
				listener <- e
			}
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
		fmt.Println("History")
		go channel.SendHistory(self.q)
	}

	for {
		select {
		case <-self.forwarder.CloseNotify():
			return
		case  event := <- self.q:
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
	self.redis.Do("ZREMRANGEBYRANK", event.Channel, 10, -1)
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

type ChannelRegistry struct {
	dict map[string]*Channel
}

func (self *ChannelRegistry) LeaveChannel(name string, c *SubscriberConnection) {
	if channel, ok := self.dict[name]; ok {
		channel.RemoveListener(c.id)
	}
}

func (self *ChannelRegistry) EnterChannel(name string, c *SubscriberConnection) (channel *Channel, err error) {
	if channel, ok := self.dict[name]; ok {
		fmt.Println("Channel", name, "exists")
		channel.AddListener(c.id, c.q)
		return channel, nil
	}

	channel, err = self.NewChannel(name)
	fmt.Println("Open new Channel \"", channel.name, "\"")

	if err != nil {
		return nil, err
	}

	channel.AddListener(c.id, c.q)
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
		listeners: map[string]chan Event{},
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
