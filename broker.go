package main

type Broker interface {
	Publish(channel string, ev *Event) error
}

type ChannelEvent struct {
	Channel string
	Event   *Event
}
