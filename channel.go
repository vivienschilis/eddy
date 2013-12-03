package main

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"sync"
)

type Channel struct {
	name      string
	psc       redis.PubSubConn
	listeners map[string]chan Event
	closing   chan bool
	wg        sync.WaitGroup
}

func NewChannel(name string) (eventChannel *Channel, err error) {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		return
	}

	eventChannel = &Channel{
		name:      name,
		psc:       redis.PubSubConn{Conn: c},
		listeners: map[string]chan Event{},
		closing:   make(chan bool),
	}

	return
}

func (self *Channel) SendHistory(id string) {
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		return
	}
	defer conn.Close()

	n, err := redis.Values(conn.Do("ZREVRANGE", self.name, 0, -1))
	if err != nil {
		return
	}

	fmt.Println("History")
	for _, x := range n {
		var e Event
		json.Unmarshal(x.([]byte), &e)
		self.listeners[id] <- e
	}
}

func (self *Channel) Wait() {
	self.wg.Wait()
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
