package consul

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/hashicorp/consul/api"
	"github.com/nsaje/dagger/dagger"
	"github.com/nsaje/dagger/s"
)

const (
	// JobPrefix is the key prefix for tasks
	taskPrefix        = "dagger/tasks/"
	subscribersPrefix = "dagger/subscribers/"
)

// DefaultConfig returns the default config for Consul
func DefaultConfig() *api.Config {
	return api.DefaultConfig()
}

// Coordinator implementation based on Consul.io
type consulCoordinator struct {
	client *api.Client
	addr   net.Addr

	sessionID    string
	sessionRenew chan struct{}
	stopCh       chan struct{}

	subscribers     map[string]*subscribersList
	subscribersLock sync.RWMutex
}

// NewCoordinator creates a new instance of Consul coordinator
func NewCoordinator(conf *api.Config) dagger.Coordinator {
	client, _ := api.NewClient(conf)
	c := &consulCoordinator{
		client:          client,
		stopCh:          make(chan struct{}),
		subscribers:     make(map[string]*subscribersList),
		subscribersLock: sync.RWMutex{},
	}
	return c
}

func (c *consulCoordinator) Start(addr net.Addr) error {
	c.addr = addr
	session := c.client.Session()
	// set session to delete our keys on invalidation
	sessionOptions := &api.SessionEntry{
		Behavior:  api.SessionBehaviorDelete,
		LockDelay: 100 * time.Millisecond,
		TTL:       "10s",
	}
	var sessionID string
	var err error
	err = backoff.RetryNotify(func() error {
		sessionID, _, err = session.Create(sessionOptions, nil)
		return err
	}, backoff.NewExponentialBackOff(), func(err error, t time.Duration) {
		log.Println("Cannot create session, retrying in", t, ". Error:", err)
	})
	if err != nil {
		return fmt.Errorf("failed to create consul session: %v", err)
	}

	// set up a long-running goroutine for renewing the session
	c.sessionRenew = make(chan struct{})
	c.sessionID = sessionID
	go session.RenewPeriodic("5s", sessionID, nil, c.sessionRenew)

	log.Println("[coordinator] Coordinator ready")

	return nil
}

// Stop destroys the Consul session, deleting all our keys
func (c *consulCoordinator) Stop() {
	close(c.sessionRenew)
	c.sessionRenew = nil
	close(c.stopCh)

	session := c.client.Session()
	log.Printf("[coordinator] Destroying session %s", c.sessionID)
	session.Destroy(c.sessionID, nil) // ignoring error, session will expire anyway
}

func (c *consulCoordinator) NewTaskWatcher() dagger.SetWatcher {
	return c.newSetWatcher(taskPrefix)
}

func (c *consulCoordinator) AcquireTask(task s.StreamID) (bool, error) {
	log.Println("[coordinator] Trying to take task: ", task)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     string(task),
		Session: c.sessionID,
	}
	acquired, _, err := kv.Acquire(pair, nil)
	return acquired, err
}

func (c *consulCoordinator) TaskAcquired(task s.StreamID) {
	log.Println("[coordinator] Releasing task: ", task)
	kv := c.client.KV()
	kv.Delete(string(task), nil)
}

func (c *consulCoordinator) ReleaseTask(task s.StreamID) (bool, error) {
	log.Println("[coordinator] Releasing task: ", task)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     string(task),
		Session: c.sessionID,
	}
	released, _, err := kv.Release(pair, nil)
	return released, err
}

// RegisterAsPublisher registers us as publishers of this stream and
func (c *consulCoordinator) RegisterAsPublisher(compID s.StreamID) {
	log.Println("[coordinator] Registering as publisher for: ", compID)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     fmt.Sprintf("dagger/publishers/%s/%s", compID, c.addr.String()),
		Session: c.sessionID,
	}
	_, _, err := kv.Acquire(pair, nil)
	if err != nil {
		log.Println("[coordinator] Error registering as publisher: ", err)
	}
}

// SubscribeTo subscribes to topics with global coordination
func (c *consulCoordinator) SubscribeTo(topic s.StreamID, from s.Timestamp) error {
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     c.constructSubscriberKey(topic),
		Value:   []byte(fmt.Sprintf("%d", from)),
		Session: c.sessionID,
	}
	// ignore bool, since if it's false, it just means we're already subscribed
	_, _, err := kv.Acquire(pair, nil)

	if strings.ContainsAny(string(topic), "()") {
		// only monitor publishers if it's a computation
		go c.monitorPublishers(topic)
	}
	return err
}

func (c *consulCoordinator) CheckpointPosition(topic s.StreamID, from s.Timestamp) error {
	log.Println("[TRACE] checkpointing position", topic, from)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     c.constructSubscriberKey(topic),
		Value:   []byte(fmt.Sprintf("%d", from)),
		Session: c.sessionID,
	}
	_, err := kv.Put(pair, nil)
	if err != nil {
		log.Println("[checkpoint] error", err)
	}
	return err
}

// UnsubscribeFrom unsubscribes from topics with global coordination
func (c *consulCoordinator) UnsubscribeFrom(topic s.StreamID) error {
	kv := c.client.KV()
	key := c.constructSubscriberKey(topic)
	_, err := kv.Delete(key, nil)
	return err
}

func (c *consulCoordinator) NewSubscribersWatcher(topic s.StreamID) dagger.SetDiffWatcher {
	return c.newSetDiffWatcher(subscribersPrefix + string(topic) + "/")
}

func (c *consulCoordinator) GetSubscriberPosition(topic s.StreamID, subscriber string) (s.Timestamp, error) {
	pair, _, err := c.client.KV().Get(subscribersPrefix+string(topic)+"/"+subscriber, nil)
	if err != nil {
		return s.Timestamp(0), err
	}
	return s.TSFromString(string(pair.Value)), nil
}

func (c *consulCoordinator) NewSubscriberPositionWatcher(topic s.StreamID, subscriber string) dagger.ValueWatcher {
	return c.newValueWatcher(subscribersPrefix + string(topic) + "/" + subscriber)
}

// ------------- OLD --------------

// JoinGroup joins the group that produces compID computation
func (c *consulCoordinator) JoinGroup(compID s.StreamID) (dagger.GroupHandler, error) {
	gh := &groupHandler{
		compID: compID,
		c:      c,
		stopCh: make(chan struct{}),
		errCh:  make(chan error),
	}
	err := gh.contend()
	go func() {
		for {
			select {
			case <-gh.stopCh:
				return
			default:
				err := gh.contend()
				if err != nil {
					gh.errCh <- err
				}
			}
		}
	}()
	return gh, err
}

type groupHandler struct {
	compID s.StreamID
	c      *consulCoordinator

	areWeLeader   bool
	currentLeader string

	lastIndex uint64
	stopCh    chan struct{}
	errCh     chan error

	sync.RWMutex
}

// GetStatus returns whether we're the leader and the address of the current leader
func (gh *groupHandler) GetStatus() (bool, string, error) {
	for {
		select {
		case err := <-gh.errCh:
			return false, "", err
		default:
			gh.RLock()
			currentLeader := gh.currentLeader
			gh.RUnlock()
			if currentLeader == "" {
				// wait until a leader is elected
				time.Sleep(time.Second)
				continue
			}
			gh.RLock()
			defer gh.RUnlock()
			return gh.areWeLeader, gh.currentLeader, nil
		}
	}
}

func (gh *groupHandler) contend() error {
	key := fmt.Sprintf("dagger/%s/publishers_leader", string(gh.compID))
	if gh.currentLeader == "" {
		pair := &api.KVPair{
			Key:     key,
			Session: gh.c.sessionID,
			Value:   []byte(gh.c.addr.String()),
		}
		kv := gh.c.client.KV()
		log.Println("[coordinator][groupHandler] Trying to acquire leadership of ", gh.compID)
		_, _, err := kv.Acquire(pair, nil)
		if err != nil {
			log.Println("Error acquiring leadership", err)
			return err
		}
	}
	return gh.fetch()
	// return nil
}

func (gh *groupHandler) fetch() error {
	key := fmt.Sprintf("dagger/%s/publishers_leader", gh.compID)
	kv := gh.c.client.KV()

	qOpts := &api.QueryOptions{WaitIndex: gh.lastIndex}
	// determine if we should do a short poll in case a leader's not chosen yet
	gh.RLock()
	if gh.currentLeader == "" {
		qOpts.WaitTime = time.Second
	}
	gh.RUnlock()
	pair, queryMeta, err := kv.Get(key, qOpts)
	log.Println("[coordinator][groupHandler] Fetch returned new data")
	if err != nil {
		log.Println("FETCH ERROR")
		return err
	}
	gh.Lock()
	gh.lastIndex = queryMeta.LastIndex
	if pair == nil || pair.Session == "" {
		gh.currentLeader = ""
		gh.areWeLeader = false
	} else {
		gh.currentLeader = string(pair.Value)
		gh.areWeLeader = (gh.currentLeader == gh.c.addr.String())
	}
	gh.Unlock()

	log.Println("[coordinator] New leader:", gh.currentLeader)

	return nil
}

func (c *consulCoordinator) monitorPublishers(topic s.StreamID) {
	prefix := fmt.Sprintf("dagger/publishers/%s", topic)
	kv := c.client.KV()
	lastIndex := uint64(0)
	lastNumPublishers := -1
	for {
		keys, queryMeta, err := kv.Keys(prefix, "", &api.QueryOptions{WaitIndex: lastIndex})
		log.Println("[coordinator] Publishers checked in ", prefix)
		if err != nil {
			log.Fatal("ERROR:", err)
			// FIXME
			continue
		}
		// log.Println("last index before, after ", lastIndex, queryMeta.LastIndex)
		lastIndex = queryMeta.LastIndex

		// if there are no publishers registered, post a new job
		if len(keys) != lastNumPublishers && len(keys) < 2 {
			log.Printf("[coordinator] Number of publishers of %s is %d, posting a job.", topic, len(keys))
			// log.Println("Publishers: ", keys)
			pair := &api.KVPair{
				Key: taskPrefix + string(topic),
			}
			kv.Put(pair, nil) // FIXME error handling
		} else { // FIXME: do this more elegantly
			if len(keys) == 2 {
				log.Printf("[coordinator] Number of publishers of %s is %d, not posting a job.", topic, len(keys))
				// log.Println("Publishers: ", keys)
			}
		}
		lastNumPublishers = len(keys)
	}
}

// GetSubscribers returns the addresses of subscribers interested in a certain topic
func (c *consulCoordinator) GetSubscribers(topic s.StreamID) ([]string, error) {
	tags := dagger.ParseTags(topic)
	log.Println("Publisher tags:", tags, topic)
	topic = dagger.StripTags(topic)
	prefix := fmt.Sprintf("dagger/subscribers/%s/", topic)

	c.subscribersLock.RLock()
	subsList := c.subscribers[prefix]
	c.subscribersLock.RUnlock()
	if subsList == nil {
		c.subscribersLock.Lock()
		subsList = c.subscribers[prefix]
		// check again, otherwise someone might have already acquired write lock before us
		if subsList == nil {
			subsList = &subscribersList{prefix: prefix, tags: tags, c: c}
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

func (c *consulCoordinator) constructSubscriberKey(topic s.StreamID) string {
	return fmt.Sprintf("dagger/subscribers/%s/%s", topic, c.addr.String())
}

type subscribersList struct {
	subscribers []string
	prefix      string
	tags        dagger.Tags
	lastAccess  time.Time
	lastIndex   uint64
	c           *consulCoordinator
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
		if time.Since(lastAccess) >= 15*time.Second {
			log.Printf("[coordinator] TTL expired for subscribers of '%s'", sl.prefix)
			sl.c.subscribersLock.Lock()
			defer sl.c.subscribersLock.Unlock()
			delete(sl.c.subscribers, sl.prefix)
			return
		}

		// do a blocking query for when our prefix is updated
		err := sl.fetch()
		if err != nil {
			log.Fatal("[coordinator][WARNING] Problem syncing subscribers for prefix:", sl.prefix)
		}
	}
}

func (sl *subscribersList) fetch() error {
	kv := sl.c.client.KV()
	// fmt.Println("Executing blocking consul.Keys method, lastIndex: ", sl.lastIndex)
	keys, queryMeta, err := kv.Keys(sl.prefix, "", &api.QueryOptions{WaitIndex: sl.lastIndex})
	log.Println("[coordinator] Subscribers updated in ", sl.prefix)
	// fmt.Println("consul.Keys method returned, New LastIndex: ", queryMeta.LastIndex)
	if err != nil {
		return err
	}
	sl.lastIndex = queryMeta.LastIndex
	subscribers := filterTags(keys, sl.tags)
	sl.subscribers = subscribers
	return nil
}

func filterTags(subscribers []string, publisherTags dagger.Tags) []string {
	filtered := make([]string, 0, len(subscribers))
	for _, key := range subscribers {
		tags := dagger.ParseTags(s.StreamID(key))
		log.Println("[coordinator][tags] Publisher", publisherTags, ", subscriber", tags)
		tagsMatch := true
		for k, v := range tags {
			if publisherTags[k] != v {
				tagsMatch = false
			}
		}
		if tagsMatch {
			idx := strings.LastIndex(key, "/")
			if idx > 0 {
				filtered = append(filtered, key[idx+1:])
			}
		}
	}
	return filtered
}
