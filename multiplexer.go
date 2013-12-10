package main

import (
	"log"
)

const (
	CLIENT_CONNECT = iota
	CLIENT_DISCONNECT
	// TODO: See if this state is useful
	QUIT
)

type Op struct {
	action   int
	channels []string
	resp     chan<- *ChannelEvent
}

func NewMultiplexer(sub BrokerSubscriber) chan<- Op {
	c := make(chan Op)
	go multiplexer(sub, c)
	return c
}

func multiplexer(sub BrokerSubscriber, comm <-chan Op) {
	registry := make(map[string]map[chan<- *ChannelEvent]bool)
	buffers := make(map[string]*ChannelBuffer)

	for {
		select {
		case state := <-sub.StateChange():
			switch state {
			case BROKER_CONNECTED:
				// Re-subscribe
				for c, _ := range registry {
					sub.Subscribe(c)
				}
				log.Println("multiplexer: broker connected")
			case BROKER_DISCONNECTED:
				log.Println("multiplexer: broker disconnected")
			}
		case op := <-comm:
			switch op.action {
			case CLIENT_CONNECT:
				log.Println("multiplexer: CONNECT", op)
				for _, c := range op.channels {
					var b *ChannelBuffer
					_, ok := registry[c]
					if !ok {
						b = NewChannelBuffer()
						registry[c] = make(map[chan<- *ChannelEvent]bool)
						buffers[c] = b

						sub.Subscribe(c)
					}
					registry[c][op.resp] = true

					b = buffers[c]
					// TODO: If the client disconnects at this point it's annoying.
					// TODO: It might also slow down the loop if the client is slow
					//       to receive these messages.
					// The buffer might flush itself if it's too old
					for _, ev := range b.Since(0) {
						op.resp <- &ChannelEvent{c, ev}
					}
				}
			case CLIENT_DISCONNECT:
				log.Println("multiplexer: DISCONNECT", op)
				for _, c := range op.channels {
					if registry[c] == nil {
						panic("BUG: missing registry channel " + c)
					}
					if buffers[c] == nil {
						panic("BUG: missing buffers channel " + c)
					}
					delete(registry[c], op.resp)
					if len(registry[c]) == 0 {
						delete(registry, c)
						delete(buffers, c)

						sub.Unsubscribe(c)
					}
				}
				close(op.resp)
			}
		case ce := <-sub.ChannelEvent():
			targets, ok := registry[ce.Channel]
			if !ok {
				log.Println("multiplexer got an event on a unused channel ", ce.Channel)
			} else {
				if buffers[ce.Channel].Add(ce.Event) {
					for c, _ := range targets {
						c <- ce
					}
				} else {
					log.Println("mulitplexer: ignoring", ce)
				}
			}
		}
	}
}
