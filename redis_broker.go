package main

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"sync"
	"time"
)

type RedisPublisher struct {
	addr string
}

func NewRedisPublisher(addr string) *RedisPublisher {
	return &RedisPublisher{addr}
}

// Each Publish initiates a new connection
func (self *RedisPublisher) Publish(channel string, event *Event) (err error) {
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

func (self *RedisPublisher) connect() (redis.Conn, error) {
	return redis.Dial("tcp", self.addr)
}

type RedisSubscriber struct {
	addr        string
	state       int
	stateChange chan int
	event       chan *ChannelEvent
	mu          sync.Mutex
	c           redis.Conn
	psc         redis.PubSubConn
}

func NewRedisSubscriber(addr string) *RedisSubscriber {
	sub := &RedisSubscriber{
		addr:        addr,
		state:       BROKER_DISCONNECTED,
		stateChange: make(chan int),
		event:       make(chan *ChannelEvent),
	}

	go func() {
		for {
			switch sub.state {
			case BROKER_DISCONNECTED:
				log.Println("redis sub: BROKER_DISCONNECTED")
				var err error
				var c1, c2 redis.Conn
				var psc redis.PubSubConn

				c1, err = redis.Dial("tcp", addr)
				if err != nil {
					log.Println("redis subscriber conn ERR: ", err)
					time.Sleep(2 * time.Second)
					continue
				}

				c2, err = redis.Dial("tcp", addr)
				if err != nil {
					log.Println("redis sub conn ERR2: ", err)
					c1.Close()
					time.Sleep(2 * time.Second)
					continue
				}
				psc = redis.PubSubConn{c2}

				sub.connect(c1, psc)
			case BROKER_CONNECTED:
				msg := sub.psc.Receive()
				log.Println("redis sub: msg", msg)
				switch msg.(type) {
				case redis.Message:
					var err error
					var ev *Event
					m := msg.(redis.Message)
					if ev, err = LoadEvent(m.Data); err != nil {
						// Just log the error, there is an issue in the redis storage
						log.Println("redis sub LoadEvent2:", m.Data, err)
					} else {
						ce := &ChannelEvent{m.Channel, ev}
						sub.event <- ce
					}
				case error:
					log.Println("redis sub redis err:", msg)
					sub.disconnect()
				default:
					panic("BUG: case missing")
				}
			}
		}
	}()

	return sub
}

// Don't subscribe twice
func (self *RedisSubscriber) Subscribe(channel string) {
	if self.state == BROKER_DISCONNECTED {
		return
	}

	if !self.ok(self.psc.Subscribe(channel)) {
		return
	}

	// Send old values on the channel
	values, err := redis.Strings(self.c.Do("ZREVRANGE", channel, 0, -1))
	if !self.ok(err) {
		return
	}

	for _, v := range values {
		var ev *Event
		if ev, err = LoadEvent([]byte(v)); err != nil {
			// TODO: Handle error
			log.Println("multiplexer: LoadEvent2:", v, err)
		} else {
			// TODO: Make this async
			self.event <- &ChannelEvent{channel, ev}
		}
	}
}

func (self *RedisSubscriber) Unsubscribe(channel string) {
	if self.state == BROKER_DISCONNECTED {
		return
	}

	self.ok(self.psc.Unsubscribe(channel))
}

func (self *RedisSubscriber) ok(err error) bool {
	if err != nil {
		log.Println("redis subscriber:", err)
		self.disconnect()
		return false
	}
	return true
}

func (self *RedisSubscriber) disconnect() {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.state == BROKER_DISCONNECTED {
		return
	}

	self.c.Close()
	self.psc.Close()
	self.state = BROKER_DISCONNECTED
	self.stateChange <- BROKER_DISCONNECTED
}

func (self *RedisSubscriber) connect(c redis.Conn, psc redis.PubSubConn) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.state == BROKER_CONNECTED {
		panic("BUG: how could that happen ?")
	}

	self.c = c
	self.psc = psc
	self.state = BROKER_CONNECTED
	self.stateChange <- BROKER_CONNECTED
}

func (self *RedisSubscriber) StateChange() <-chan int {
	return self.stateChange
}

func (self *RedisSubscriber) ChannelEvent() <-chan *ChannelEvent {
	return self.event
}
