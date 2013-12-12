package main

import (
	"testing"
	"time"
)

func TestChannelBuffer(t *testing.T) {
	e1 := NewEvent(1337, 1, 2, "")
	e2 := NewEvent(1338, 1, 2, "")
	e3 := NewEvent(1339, 1, 2, "")

	b := NewChannelBuffer()

	if !b.Add(e1) {
		t.Error("add e1 failed")
	}
	if b.oldest() != e1 {
		t.Error("e1 is not the oldest")
	}
	if b.newest() != e1 {
		t.Error("e1 is not the newest")
	}
	if b.Len() != 1 {
		t.Error("Len should == 1")
	}

	if b.Add(e1) {
		t.Error("e1 should not add again")
	}
	if !b.Add(e2) {
		t.Error("could not add e2")
	}
	if b.oldest() != e1 {
		t.Error("oldest should still be e1")
	}
	if b.newest() != e2 {
		t.Error("newest should be e2 now")
	}
	if b.Len() != 2 {
		t.Error("len should == 2")
	}

	if b.Add(e1) {
		t.Error("x")
	}
	if b.Add(e2) {
		t.Error("y")
	}

	if !b.Add(e3) {
		t.Error("z")
	}
	if b.oldest() != e2 {
		t.Error("a")
	}
	if b.newest() != e3 {
		t.Error("b")
	}
	if b.Len() != 2 {
		t.Error("c")
	}

	if b.Add(e1) {
		t.Error("d")
	}
}

func TestChannelBufferExpiry(t *testing.T) {
	TTL := 1
	t1 := time.Unix(1337, 0)
	t2 := time.Unix(1338, 0)
	t3 := time.Unix(1900, 0)
	c := NewChannelBuffer()

	if c.hasExpired(t1) {
		t.Error("oops, should never expired when empty")
	}

	c.Add(NewEvent(t1.UnixNano(), TTL, 2, ""))
	c.Add(NewEvent(t2.UnixNano(), TTL, 2, ""))

	if c.Len() != 2 {
		t.Error("len is now right")
	}

	if c.hasExpired(t1) {
		t.Error("oops t1")
	}

	if c.hasExpired(t2) {
		t.Error("oops t2")
	}

	if !c.hasExpired(t3) {
		t.Error("oops t3")
	}
}

