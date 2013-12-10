package main

const (
	BROKER_CONNECTED = iota
	BROKER_DISCONNECTED
)

type BrokerPublisher interface {
	Publish(channel string, ev *Event) error
}

// The interface is pretty weak and only works with a single user
type BrokerSubscriber interface {
	Subscribe(channel string)
	Unsubscribe(channel string)
	StateChange() <-chan int
	ChannelEvent() <-chan *ChannelEvent
}

type ChannelEvent struct {
	Channel string
	Event   *Event
}
