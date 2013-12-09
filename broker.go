package main

type Broker interface {
	Publish(ev *Event) error
}
