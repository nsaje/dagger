package main

import "github.com/hashicorp/consul/api"

// Coordinator coordinates topics, their publishers and subscribers
type Coordinator interface {
	RegisterTopic(string) error
	// GetSubscribers(string) ([]string, error)
	GetPublishers(string) ([]string, error)
	// GetConfig(string) (string, error)
}

// ConsulCoordinator implementation based on Consul.io
type ConsulCoordinator struct {
	client *api.Client
	kv     *api.KV
}

// RegisterTopic registers topics with global coordination
func (c *ConsulCoordinator) RegisterTopic(topic string) error {
	p := &api.KVPair{Key: topic, Value: []byte("test")}
	_, err := c.kv.Put(p, nil)
	return err
}

// GetPublishers returns the addresses of publishers that publish a certain topic
func (c *ConsulCoordinator) GetPublishers(topic string) ([]string, error) {
	pair, _, err := c.kv.Get(topic, nil)
	if err != nil {
		return nil, err
	}
	return []string{string(pair.Value)}, nil
}

// this may return different coordinators based on config in the future
func newCoordinator() (Coordinator, error) {
	client, _ := api.NewClient(api.DefaultConfig())
	kv := client.KV()
	c := &ConsulCoordinator{client, kv}
	return c, nil
}
