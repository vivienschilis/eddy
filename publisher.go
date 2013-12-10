package main

import (
	"log"
	"net/http"
	"time"
)

type PublisherHandler struct {
	pub BrokerPublisher
}

const BUF_EXPIRE = 3600
const BUF_SIZE = 10

func NewPublisherHandler(pub BrokerPublisher) *PublisherHandler {
	return &PublisherHandler{pub}
}

func (self *PublisherHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var err error

	channel := req.URL.Query().Get("channel")
	data := req.URL.Query().Get("data")
	timestamp := time.Now().UnixNano()

	// TODO: Accept these as parameters
	size := BUF_SIZE
	ttl := BUF_EXPIRE

	// TODO: Fail if we don't have any channel or data

	err = self.pub.Publish(channel, NewEvent(timestamp, ttl, size, data))
	if err != nil {
		log.Println("publisher: ", err)
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(201)
}
