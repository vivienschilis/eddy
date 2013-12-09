package main

import (
	"encoding/json"
)

type Event struct {
	At   int64  `json:"at"`
	TTL  int    `json:"ttl"`
	Size int    `json:"size"`
	Data string `json:"data"`
}

func NewEvent(at int64, ttl int, size int, data string) *Event {
	return &Event{at, ttl, size, data}
}

func LoadEvent(data []byte) (e *Event, err error) {
	e = new(Event)
	err = json.Unmarshal(data, e)
	return
}

func DumpEvent(event *Event) (data []byte) {
	data, err := json.Marshal(event)
	if err != nil {
		panic("BUG: " + err.Error())
	}
	return data
}
