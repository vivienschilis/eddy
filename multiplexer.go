package main

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

const (
	CLIENT_CONNECT = iota
	CLIENT_DISCONNECT
	// TODO: See if this state is useful
	QUIT
)

const (
	REDIS_CONNECTED = iota
	REDIS_DISCONNECTED
)

type Op struct {
	action   int
	channels []string
	resp     chan<- *ChannelEvent
}

func NewMultiplexer(redisAddr string) chan<- Op {
	c := make(chan Op)
	go multiplexer(redisAddr, c)
	return c
}

func multiplexer(redisAddr string, comm <-chan Op) {
	var err error
	var redisC redis.Conn
	var redisPSC redis.PubSubConn

	redisMsg := make(chan interface{})
	registry := make(map[string]map[chan<- *ChannelEvent]bool)
	//buffers := make(map[string]*ChannelBuffer)
	state := REDIS_DISCONNECTED

	for {
		switch state {
		case REDIS_DISCONNECTED:
			log.Println("multiplexer: REDIS_DISCONNECTED")
			redisC, err = redis.Dial("tcp", redisAddr)
			if err != nil {
				log.Println("multiplexer ERR: ", err)
				time.Sleep(2 * time.Second)
				continue
			}

			var c redis.Conn
			c, err = redis.Dial("tcp", redisAddr)
			if err != nil {
				log.Println("multiplexer ERR2: ", err)
				time.Sleep(2 * time.Second)
				redisC.Close()
				continue
			}
			redisPSC = redis.PubSubConn{c}

			go func(psc redis.PubSubConn, c chan interface{}) {
				redisMsg = make(chan interface{})
				for {
					msg := psc.Receive()
					c <- msg

					switch msg.(type) {
					case error:
						return
					}
				}
			}(redisPSC, redisMsg)

			log.Println("multiplexer: REDIS_CONNECTED")
			state = REDIS_CONNECTED
		case REDIS_CONNECTED:
			var op Op
			var msg interface{}
			select {
			case op = <-comm:
				switch op.action {
				case CLIENT_CONNECT:
					log.Println("multiplexer: CONNECT", op)
					for _, c := range op.channels {
						_, ok := registry[c]
						if !ok {
							registry[c] = make(map[chan<- *ChannelEvent]bool)

							// TODO: Handle subscription error
							redisPSC.Subscribe(c)
						}
						registry[c][op.resp] = true

						// TODO: Put these values into an intermediate buffer
						// Here we send the backlog we get from redis
						values, err := redis.Strings(redisC.Do("ZREVRANGE", c, 0, -1))
						if err != nil {
							log.Println("multiplexer ZREVRANGE:", err)
							// TODO: Deal with the error
							break
						}
						// TODO: If the client disconnects at this point it's annoying.
						// TODO: It might also slow down the loop if the client is slow
						//       to receive these messages.
						for _, v := range values {
							var ev *Event
							if ev, err = LoadEvent([]byte(v)); err != nil {
								// TODO: Handle error
								log.Println("multiplexer: LoadEvent:", v, err)
							} else {
								op.resp <- &ChannelEvent{c, ev}
							}
						}
					}
				case CLIENT_DISCONNECT:
					log.Println("multiplexer: DISCONNECT", op)
					for _, c := range op.channels {
						if registry[c] == nil {
							panic("BUG: missing registry channel " + c)
						}
						delete(registry[c], op.resp)
						if len(registry[c]) == 0 {
							redisPSC.Unsubscribe(c)
							// TODO: Error checking
							// TODO: Flush local buffer if we have any
							delete(registry, c)
						}
					}
					// TODO: We might be loosing some events between the disconnect and
					//			 reconnect.
				case QUIT:
					log.Println("multiplexer: BYE")
					redisC.Close()
					redisPSC.Close()
					return
				default:
					panic("BUG: case missing")
				}
			case msg = <-redisMsg:
				log.Println("multiplexer: msg", msg)
				switch msg.(type) {
				case redis.Message:
					var ev *Event
					m := msg.(redis.Message)
					if ev, err = LoadEvent(m.Data); err != nil {
						// Just log the error, there is an issue in the redis storage
						log.Println("multiplexer LoadEvent2:", m.Data, err)
					} else {
						targets, ok := registry[m.Channel]
						if !ok {
							log.Println("multiplexer got an event on a unused channel ", m.Channel)
						} else {
							cev := &ChannelEvent{m.Channel, ev}
							for c, _ := range targets {
								c <- cev
							}
						}
					}
				case error:
					log.Println("multiplexer redis err:", msg)
					redisC.Close()
					redisPSC.Close()
					// FIXME: We might loose some messages unless we fetch the buffers
					state = REDIS_DISCONNECTED
				default:
					panic("BUG: case missing")
				}
			}
		}
	}
}
