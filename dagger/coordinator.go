package dagger

import (
	"fmt"
	"log"
	"net"

	"github.com/hashicorp/consul/api"
)

// Coordinator coordinates topics, their publishers and subscribers
type Coordinator interface {
	SubscribeTo(string) error
	// GetSubscribers(string) ([]string, error)
	GetSubscribers(string) ([]string, error)
	// GetConfig(string) (string, error)
	Start() error
	Stop()
}

// ConsulCoordinator implementation based on Consul.io
type ConsulCoordinator struct {
	client   *api.Client
	nodeConf *Config
	addr     net.Addr

	sessionID    string
	sessionRenew chan struct{}
}

// NewCoordinator : this may return different coordinators based on config in the future
func NewCoordinator(nodeConf *Config, addr net.Addr) Coordinator {
	client, _ := api.NewClient(api.DefaultConfig())
	c := &ConsulCoordinator{
		client:   client,
		nodeConf: nodeConf,
		addr:     addr,
	}
	return c
}

// Start creates a new session and starts synchronizing state
func (c *ConsulCoordinator) Start() error {
	session := c.client.Session()
	// set session to delete our keys on invalidation
	sessionOptions := &api.SessionEntry{Behavior: api.SessionBehaviorDelete}
	sessionID, _, err := session.Create(sessionOptions, nil)
	if err != nil {
		return fmt.Errorf("failed to create consul session: %v", err)
	}

	// set up a long-running goroutine for renewing the session
	c.sessionRenew = make(chan struct{})
	c.sessionID = sessionID
	go session.RenewPeriodic(api.DefaultLockSessionTTL, sessionID, nil, c.sessionRenew)

	return nil
}

// Stop destroys the Consul session, deleting all our keys
func (c *ConsulCoordinator) Stop() {
	close(c.sessionRenew)
	c.sessionRenew = nil

	session := c.client.Session()
	log.Printf("Destroying session %s", c.sessionID)
	session.Destroy(c.sessionID, nil) // ignoring error, session will expire anyway
}

// SubscribeTo subscribes to topics with global coordination
func (c *ConsulCoordinator) SubscribeTo(topic string) error {
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     c.constructKey(topic),
		Session: c.sessionID,
	}
	// ignore bool, since if it false it just means we're already subscribed
	_, _, err := kv.Acquire(pair, nil)
	return err
}

// GetSubscribers returns the addresses of subscribers interested in a certain topic
func (c *ConsulCoordinator) GetSubscribers(topic string) ([]string, error) {
	kv := c.client.KV()
	prefix := fmt.Sprintf("dagger/%s/subscribers/", topic)
	keys, _, err := kv.Keys(prefix, "", nil)
	if err != nil {
		return nil, err
	}
	subscribers := make([]string, len(keys))
	for i, key := range keys {
		subscribers[i] = key[len(prefix):]
	}
	return subscribers, nil
}

func (c *ConsulCoordinator) constructKey(topic string) string {
	return fmt.Sprintf("dagger/%s/subscribers/%s", topic, c.addr.String())
}
