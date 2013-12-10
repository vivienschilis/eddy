package main

import (
	"net/http"
	"strings"
	"time"
)

var DATA_HEADER = []byte("data: ")
var END_OF_MESSAGE = []byte("\n\n")

// HACK: There is no true noop in the spec
var NOOP = []byte("\n")

type SubscriberHandler struct {
	multiplexer chan<- Op
}

type SubscriberConn struct {
	writer http.ResponseWriter
	http.Flusher
	http.CloseNotifier
}

func (c SubscriberConn) SendData(data []byte) {
	// Ignoring errors here. We should get a CloseNotify event soon anyways.
	// Or are there other classes of events ?
	c.writer.Write(DATA_HEADER)
	c.writer.Write(data)
	c.writer.Write(END_OF_MESSAGE)
	c.Flush()
}

func (c SubscriberConn) SendNoop() {
	c.writer.Write(NOOP)
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

	// TODO: Fail if no channels are selected ?
	channels := strings.Split(req.URL.Query().Get("channels"), ",")
	connected := true
	conn := SubscriberConn{w, f, cn}
	resp := make(chan *ChannelEvent)

	self.multiplexer <- Op{CLIENT_CONNECT, channels, resp}

	for {
		select {
		case ce, ok := <-resp:
			if ok {
				// Forward event to client
				data := DumpEvent(ce.Event)
				// TODO: Send the channel as well
				conn.SendData(data)
			} else {
				// Multiplexer got the message and disconnected us
				connected = false
			}
		case <-time.After(5 * time.Second):
			// Send a little noop to the client
			// This is not part of the SSE spec
			conn.SendNoop()
		case <-conn.CloseNotify():
			self.multiplexer <- Op{CLIENT_DISCONNECT, channels, resp}
		}
		if !connected {
			return
		}
	}
}
