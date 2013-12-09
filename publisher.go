package main

import (
	"log"
	"net/http"
	"time"
)

type PublisherHandler struct {
	broker Broker
}

const BUF_EXPIRE = 3600
const BUF_SIZE = 10

func NewPublisherHandler(broker Broker) *PublisherHandler {
	return &PublisherHandler{broker}
}

func (self *PublisherHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var err error

	channel := req.URL.Query().Get("channel")
	data := req.URL.Query().Get("data")
	timestamp := time.Now().UnixNano()
	// TODO: Fail if we don't have any channel or data

	err = self.broker.Publish(&Event{timestamp, channel, data})
	if err != nil {
		log.Println("publisher: ", err)
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(201)
}
