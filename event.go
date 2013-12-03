package main

import (
	"encoding/json"
)

type Event struct {
	Id      int64  `json:"id"`
	Channel string `json:"name"`
	Data    string `json:"data"`
}

func LoadEvent(data []byte) (e *Event, err error) {
	e = new(Event)
	err = json.Unmarshal(data, e)
	return
}

func DumpEvent(event *Event) (data []byte, err error) {
	return json.Marshal(event)
}
