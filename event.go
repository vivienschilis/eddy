package main

import (
	"encoding/json"
)

type Event struct {
	Id      int64  `json:"id"`
	Channel string `json:"name"`
	Data    string `json:"data"`
}

func NewEvent(id int64, channel string, data string) *Event {
	return &Event{id, channel, data}
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
