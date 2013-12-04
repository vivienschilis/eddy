package main

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"net/http"
	"time"
)

type PublisherHandler struct {
	redisAddr string
}

const BUF_EXPIRE = 3600
const BUF_SIZE = 10

func NewPublisherHandler(redisAddr string) *PublisherHandler {
	return &PublisherHandler{redisAddr}
}

func (self *PublisherHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var err error

	channel := req.URL.Query().Get("channel")
	data := req.URL.Query().Get("data")
	timestamp := time.Now().UnixNano()
	// TODO: Fail if we don't have any channel or data

	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		log.Println("publisher: ", err)
		w.WriteHeader(500)
		return
	}
	defer c.Close()

	err = sendEvent(c, &Event{timestamp, channel, data})
	if err != nil {
		log.Println("publisher: ", err)
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(201)
}

func sendEvent(c redis.Conn, event *Event) (err error) {
	b, err := DumpEvent(event)
	if err != nil {
		return
	}
	data := []byte(b)

	// TODO: Error checking
	c.Do("ZADD", event.Channel, -1*event.Id, data)
	c.Do("ZREMRANGEBYRANK", event.Channel, BUF_SIZE, -1)
	c.Do("EXPIRE", event.Channel, BUF_EXPIRE)
	c.Do("PUBLISH", event.Channel, data)

	return nil
}
