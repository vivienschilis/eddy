package main

import (
	"log"
	"net/http"
	"strings"
	"time"
)

var DATA_HEADER = []byte("data: ")
var END_OF_MESSAGE = []byte("\n\n")

type SubscriberHandler struct {
	multiplexer chan<- Op
}

type SubscriberConn struct {
	writer http.ResponseWriter
	http.Flusher
	http.CloseNotifier
}

func (c SubscriberConn) Write(data []byte) {
	// TODO: Handle errors ?
	c.writer.Write(DATA_HEADER)
	c.writer.Write(data)
	c.writer.Write(END_OF_MESSAGE)
	c.Flush()
}

func NewSubscriberHandler(op chan<- Op) *SubscriberHandler {
	return &SubscriberHandler{op}
}

func (self *SubscriberHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	f, f_ok := w.(http.Flusher)
	if !f_ok {
		panic("BUG: ResponseWriter is not a Flusher")
	}

	cn, cn_ok := w.(http.CloseNotifier)
	if !cn_ok {
		panic("BUG: ResponseWriter is not a CloseNotifier")
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(200)

	channels := strings.Split(req.URL.Query().Get("channels"), ",") // TODO: Fail if no channels are selected ?
	closed := false
	conn := SubscriberConn{w, f, cn}
	resp := make(chan *Event)

	self.multiplexer <- Op{CONNECT, channels, resp}

	for {
		if closed {
			select {
			case <-resp:
				// Make sure to discard all the events in the channel before ending
			case <-time.After(2 * time.Second):
				// Bye
				return
			}
		} else {
			select {
			case event := <-resp:
				// Forward event to client
				data, err := DumpEvent(event)
				if err != nil {
					// TODO: Handle error
					log.Println("subscriber:", err)
				} else {
					// TODO: Handle error
					conn.Write(data)
				}
			case <-conn.CloseNotify():
				self.multiplexer <- Op{DISCONNECT, channels, resp}
				closed = true
			case <-time.After(5 * time.Second):
				// Send a little noop to the client
			}
		}
	}
}
