package dagger

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

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
	client *api.Client
	config *Config
	addr   net.Addr

	sessionID    string
	sessionRenew chan struct{}

	subscribers     map[string]*subscribersList
	subscribersLock sync.RWMutex
}

// NewCoordinator : this may return different coordinators based on config in the future
func NewCoordinator(config *Config, addr net.Addr) Coordinator {
	client, _ := api.NewClient(api.DefaultConfig())
	c := &ConsulCoordinator{
		client:      client,
		config:      config,
		addr:        addr,
		subscribers: make(map[string]*subscribersList),
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
		Key:     c.constructSubscriberKey(topic),
		Session: c.sessionID,
	}
	// ignore bool, since if it false it just means we're already subscribed
	_, _, err := kv.Acquire(pair, nil)
	return err
}

// GetSubscribers returns the addresses of subscribers interested in a certain topic
func (c *ConsulCoordinator) GetSubscribers(topic string) ([]string, error) {
	prefix := fmt.Sprintf("dagger/%s/subscribers/", topic)
	c.subscribersLock.RLock()
	subsList := c.subscribers[prefix]
	c.subscribersLock.RUnlock()
	if subsList == nil {
		c.subscribersLock.Lock()
		subsList = c.subscribers[prefix]
		// check again, otherwise someone might have already acquired write lock before us
		if subsList == nil {
			subsList = &subscribersList{prefix: prefix, c: c}
			log.Println("subs list: ", subsList)
			err := subsList.fetch()
			if err != nil {
				return nil, err
			}
			c.subscribers[prefix] = subsList
			// keep subscribers updated and clean up if unused
			go subsList.sync()
		}
		c.subscribersLock.Unlock()
	}
	return subsList.get(), nil
}

func (c *ConsulCoordinator) constructSubscriberKey(topic string) string {
	return fmt.Sprintf("dagger/%s/subscribers/%s", topic, c.addr.String())
}

type subscribersList struct {
	subscribers []string
	prefix      string
	lastAccess  time.Time
	lastIndex   uint64
	c           *ConsulCoordinator
	sync.RWMutex
}

func (sl *subscribersList) get() []string {
	sl.Lock()
	sl.lastAccess = time.Now()
	sl.Unlock()
	return sl.subscribers
}

func (sl *subscribersList) sync() {
	for {
		// check if this list is expired
		sl.RLock()
		lastAccess := sl.lastAccess
		sl.RUnlock()
		if time.Since(lastAccess) >= sl.c.config.SubscribersTTL {
			log.Printf("TTL expired for subscribers of '%s'", sl.prefix)
			sl.c.subscribersLock.Lock()
			defer sl.c.subscribersLock.Unlock()
			delete(sl.c.subscribers, sl.prefix)
			return
		}

		// do a blocking query for when our prefix is updated
		err := sl.fetch()
		if err != nil {
			log.Printf("WARNING: problem syncing subscribers for prefix: %s", sl.prefix)
		}
	}
}

func (sl *subscribersList) fetch() error {
	kv := sl.c.client.KV()
	fmt.Println("Executing blocking consul.Keys method, lastIndex: ", sl.lastIndex)
	keys, queryMeta, err := kv.Keys(sl.prefix, "", &api.QueryOptions{WaitIndex: sl.lastIndex})
	fmt.Println("consul.Keys method returned, New LastIndex: ", queryMeta.LastIndex)
	if err != nil {
		return err
	}
	sl.lastIndex = queryMeta.LastIndex
	subscribers := make([]string, len(keys))
	for i, key := range keys {
		subscribers[i] = key[len(sl.prefix):]
	}
	sl.subscribers = subscribers
	return nil
}
