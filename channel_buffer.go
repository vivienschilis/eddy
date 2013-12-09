package main

import (
	"time"
)

// Allows to have keep buffers of a channel data
type ChannelBuffer struct {
	buffer map[int64]*Event
	last   *Event
}

// FIXME: Once a buffer has grown, the buffer will never resize down
func NewChannelBuffer() *ChannelBuffer {
	return &ChannelBuffer{
		buffer: make(map[int64]*Event),
	}
}

func (self *ChannelBuffer) HasExpired() bool {
	e := self.last
	if e == nil {
		return false
	}
	return e.At+(int64(e.TTL)*10e6) > time.Now().UnixNano()
}

func (self *ChannelBuffer) Add(e *Event) {
	if self.last != nil && e.At > self.last.At {
		self.last = e
	}

	// Remove the last item in the buffer
	// Also make place for the new item to avoid growing the buffer unecessarily
	for len(self.buffer) > self.last.Size {
		ev := self.First()
		if ev == nil {
			panic("BUG, ev should not be nil")
		}
		delete(self.buffer, ev.At)
	}

	self.buffer[e.At] = e
}

func (self *ChannelBuffer) First() *Event {
	var first *Event
	for at, e := range self.buffer {
		if first == nil || first.At > at {
			first = e
		}
	}
	return first
}

func (self *ChannelBuffer) Last() *Event {
	return self.last
}

func (self *ChannelBuffer) Since(since int64) []*Event {
	list := make([]*Event, 0, len(self.buffer))
	for at, e := range self.buffer {
		if at > since {
			list = append(list, e)
		}
	}
	return list
}
