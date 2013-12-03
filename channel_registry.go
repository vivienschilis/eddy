package main

import (
	"fmt"
)

var chanRegistry *ChannelRegistry

type ChannelRegistry struct {
	dict map[string]*Channel
}

func (self *ChannelRegistry) LeaveChannel(name string, c *SubscriberConnection) {
	if channel, ok := self.dict[name]; ok {
		channel.RemoveListener(c.id)
	}
}

func (self *ChannelRegistry) EnterChannel(name string, c *SubscriberConnection) (channel *Channel, err error) {
	if channel, ok := self.dict[name]; ok {
		fmt.Println("Channel", name, "exists")
		channel.AddListener(c.id, c.q)
		return channel, nil
	}

	channel, err = NewChannel(name)
	fmt.Println("Open new Channel \"", channel.name, "\"")

	if err != nil {
		return nil, err
	}

	channel.AddListener(c.id, c.q)
	self.dict[name] = channel

	go channel.run()

	go func(c *Channel) {
		channel.Wait()
		delete(self.dict, c.name)
		fmt.Println("Close Channel \"", c.name, "\"")
		c.Close()
	}(channel)

	return channel, nil
}

func init() {
	chanRegistry = &ChannelRegistry{
		make(map[string]*Channel),
	}

}
