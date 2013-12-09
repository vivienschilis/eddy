package main

import (
	"time"
)

// Allows to have keep blob buffers of a channel data
// Performance-wise, the buffers are supposed to be quite small so
//  map vs [] doesn't really matter.
type ChannelBuffer struct {
	buffer map[int64]*Event
	last   *Event
}

// FIXME: Once a buffer has grown, the buffer will never resize down
func NewChannelBuffer() *ChannelBuffer {
	c := new(ChannelBuffer)
	c.flush()
	return c
}

// Not really a push, events might arrive at any order
//
// Returns true if the event was added to the buffer
func (self *ChannelBuffer) Add(e *Event) bool {
	// FIXME: Handle case where two different events have the same timestamp ?
	if self.buffer[e.At] != nil {
		return false
	}

	if e.Size <= 0 {
		panic("BUG: The event.Size must be greater than zero")
	}

	if self.last == nil || e.At > self.last.At {
		self.last = e
	}

	// Skip adding the event if it's too old and the buffer has grown
	if len(self.buffer) >= self.last.Size {
		first := self.Oldest()
		if first != nil && first.At > e.At {
			if self.last == e {
				panic("BUG: cannot be last with a full buffer")
			}
			return false
		}
	}

	// Remove the last item in the buffer
	// Also make place for the new item to avoid growing the buffer unecessarily
	for len(self.buffer) > self.last.Size {
		ev := self.Oldest()
		if ev == nil {
			panic("BUG, ev should not be nil")
		}
		// Possible weird corner-case ?
		if ev.At > e.At {
			return false
		}
		delete(self.buffer, ev.At)
	}

	self.buffer[e.At] = e

	return true
}

func (self *ChannelBuffer) Oldest() *Event {
	var first *Event
	for at, e := range self.buffer {
		if first == nil || first.At > at {
			first = e
		}
	}
	return first
}

func (self *ChannelBuffer) Since(since int64) []*Event {
	if self.hasExpired() {
		self.flush()
		return nil
	}

	list := make([]*Event, 0, len(self.buffer))
	for at, e := range self.buffer {
		if at > since {
			list = append(list, e)
		}
	}
	return list
}

func (self *ChannelBuffer) flush() {
	self.last = nil
	self.buffer = make(map[int64]*Event)
}

func (self *ChannelBuffer) hasExpired() bool {
	e := self.last
	if e == nil {
		return false
	}
	return e.At+(int64(e.TTL)*10e6) > time.Now().UnixNano()
}
