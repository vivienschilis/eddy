package main

import (
	"crypto/sha1"
	"fmt"
	"time"
)

var uuid *UUID

type UUID struct {
	id chan string
}

func (self *UUID) Generate(N int) (uuid string) {
	uuid = <-self.id
	return uuid[0:(N - 1)]
}

func (self *UUID) run() {
	h := sha1.New()
	c := []byte(time.Now().String())

	for {
		h.Write(c)
		self.id <- fmt.Sprintf("%x", h.Sum(nil))
	}
}

func init() {
	uuid = &UUID{
		id: make(chan string),
	}

	go uuid.run()
}
