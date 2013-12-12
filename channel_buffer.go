package main

import (
	"sort"
	"time"
)

// Allows to have keep blob buffers of a channel data
// Performance-wise, the buffers are supposed to be quite small so
//  map vs [] doesn't really matter.
type ChannelBuffer struct {
	buf []*Event
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
//
// TODO: I'm sure there is a more efficient algorithm out there
func (self *ChannelBuffer) Add(e *Event) bool {
	if e.Size <= 0 {
		panic("BUG: The event.Size must be greater than zero")
	}

	if e.TTL <= 0 {
		panic("BUG: The event.TTL must be greater than zero")
	}

	for _, ev := range self.buf {
		// FIXME: Compare events not by pointer
		if ev == e {
			return false
		}
	}

	newest := self.newest()
	if newest == nil || e.At > newest.At {
		newest = e
	}

	// Skip adding the event if it's too old and the buffer has grown
	if len(self.buf) >= newest.Size {
		for _, ev := range self.buf[len(self.buf)-newest.Size:] {
			if e.At < ev.At {
				return false
			}
		}
	}

	// Remove the last item in the buffer
	// Also make place for the new item to avoid growing the buffer unecessarily
	if len(self.buf) >= newest.Size {
		self.buf = self.buf[:newest.Size-1]
	}

	// Finally. Add our event
	self.buf = append(self.buf, e)

	// Make sure it's in order
	sort.Sort(self)

	return true
}

func (self *ChannelBuffer) newest() *Event {
	if len(self.buf) == 0 {
		return nil
	}
	return self.buf[0]
}

func (self *ChannelBuffer) oldest() *Event {
	if len(self.buf) == 0 {
		return nil
	}
	return self.buf[len(self.buf)-1]
}

func (self *ChannelBuffer) All() []*Event {
	if self.hasExpired(time.Now()) {
		self.flush()
	}

	return self.buf
}

func (self *ChannelBuffer) Len() int {
	return len(self.buf)
}

func (self *ChannelBuffer) Less(i, j int) bool {
	return self.buf[i].At > self.buf[j].At
}

func (self *ChannelBuffer) Swap(i, j int) {
	x := self.buf[i]
	self.buf[i] = self.buf[j]
	self.buf[j] = x
}

func (self *ChannelBuffer) flush() {
	self.buf = []*Event{}
}

func (self *ChannelBuffer) hasExpired(now time.Time) bool {
	e := self.newest()
	if e == nil {
		return false
	}
	return now.After(time.Unix(0, e.At).Add(time.Duration(e.TTL) * time.Second))
}
