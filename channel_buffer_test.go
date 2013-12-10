package main

import (
	"testing"
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
