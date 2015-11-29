package dagger

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/hashicorp/consul/api"
)

const (
	// JobPrefix is the key prefix for tasks
	taskPrefix        = "dagger/tasks/"
	subscribersPrefix = "dagger/subscribers/"
)

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

// NewConsulCoordinator creates a new instance of Consul coordinator
func NewConsulCoordinator(customizeConfig func(*api.Config)) Coordinator {
	conf := api.DefaultConfig()
	if customizeConfig != nil {
		customizeConfig(conf)
	}
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

func (c *consulCoordinator) WatchTasks(stop chan struct{}) (chan []string, chan error) {
	return c.watchSet(taskPrefix, stop)
}

func (c *consulCoordinator) AcquireTask(task StreamID) (bool, error) {
	log.Println("[coordinator] Trying to take task: ", task)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     string(task),
		Session: c.sessionID,
	}
	acquired, _, err := kv.Acquire(pair, nil)
	return acquired, err
}

func (c *consulCoordinator) TaskAcquired(task StreamID) error {
	log.Println("[coordinator] Task acquired, deleting: ", task)
	kv := c.client.KV()
	_, err := kv.Delete(string(task), nil)
	return err
}

func (c *consulCoordinator) ReleaseTask(task StreamID) (bool, error) {
	log.Println("[coordinator] Releasing task: ", task)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     string(task),
		Session: c.sessionID,
	}
	released, _, err := kv.Release(pair, nil)
	return released, err
}

// SubscribeTo subscribes to topics with global coordination
func (c *consulCoordinator) SubscribeTo(topic StreamID, from Timestamp) error {
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     c.constructSubscriberKey(topic),
		Value:   []byte(fmt.Sprintf("%d", from)),
		Session: c.sessionID,
	}
	// ignore bool, since if it's false, it just means we're already subscribed
	_, _, err := kv.Acquire(pair, nil)
	return err
}

func (c *consulCoordinator) EnsurePublisherNum(topic StreamID, n int, stop chan struct{}) chan error {
	prefix := fmt.Sprintf("dagger/publishers/%s", topic)
	lastNumPublishers := -1
	kv := c.client.KV()
	new, errc := c.watchSet(prefix, nil)
	go func() {
		for {
			select {
			case keys := <-new:
				// if there are no publishers registered, post a new job
				if len(keys) != lastNumPublishers && len(keys) < 2 {
					log.Printf("[coordinator] Number of publishers of %s is %d, posting a job.", topic, len(keys))
					// log.Println("Publishers: ", keys)
					pair := &api.KVPair{
						Key: taskPrefix + string(topic),
					}
					_, err := kv.Put(pair, nil)
					if err != nil {
						errc <- fmt.Errorf("consul error: %s", err)
					}
				} else { // FIXME: do this more elegantly
					if len(keys) == 2 {
						log.Printf("[coordinator] Number of publishers of %s is %d, not posting a job.", topic, len(keys))
						// log.Println("Publishers: ", keys)
					}
				}
				lastNumPublishers = len(keys)
			}
		}
	}()
	return errc
}

func tagsMatch(subscriber string, publisherTags Tags) bool {
	tags := ParseTags(StreamID(subscriber))
	log.Println("[coordinator][tags] Publisher", publisherTags, ", subscriber", tags)
	for k, v := range tags {
		if publisherTags[k] != v {
			return false
		}
	}
	return true
}

func (c *consulCoordinator) CheckpointPosition(topic StreamID, from Timestamp) error {
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
func (c *consulCoordinator) UnsubscribeFrom(topic StreamID) error {
	kv := c.client.KV()
	key := c.constructSubscriberKey(topic)
	_, err := kv.Delete(key, nil)
	return err
}

func (c *consulCoordinator) WatchSubscribers(streamID StreamID, stop chan struct{}) (chan string, chan string, chan error) {
	topic := StripTags(streamID)
	publisherTags := ParseTags(streamID)
	added, dropped, errc := c.watchSetDiff(subscribersPrefix+string(topic), stop)
	filtered := make(chan string)
	filteredDropped := make(chan string)
	go func() {
		for {
			select {
			case <-stop:
				return
			case sub := <-added:
				if tagsMatch(sub, publisherTags) {
					idx := strings.LastIndex(sub, "/")
					filtered <- sub[idx+1:]
				}
			case sub := <-dropped:
				idx := strings.LastIndex(sub, "/")
				filteredDropped <- sub[idx+1:]
			}
		}
	}()
	return filtered, filteredDropped, errc
}

func (c *consulCoordinator) GetSubscriberPosition(topic StreamID, subscriber string) (Timestamp, error) {
	pair, _, err := c.client.KV().Get(subscribersPrefix+string(topic)+"/"+subscriber, nil)
	if err != nil {
		return Timestamp(0), err
	}
	return TSFromString(string(pair.Value)), nil
}

func (c *consulCoordinator) WatchSubscriberPosition(topic StreamID, subscriber string, stop chan struct{}) (chan Timestamp, chan error) {
	new, errc := c.watchValue(subscribersPrefix+string(topic)+"/"+subscriber, stop)
	posc := make(chan Timestamp)
	go func() {
		for {
			select {
			case <-stop:
				return
			case v := <-new:
				posc <- TSFromString(string(v))
			}
		}
	}()
	return posc, errc
}

// RegisterAsPublisher registers us as publishers of this stream and
func (c *consulCoordinator) RegisterAsPublisher(compID StreamID) error {
	log.Println("[coordinator] Registering as publisher for: ", compID)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     fmt.Sprintf("dagger/publishers/%s/%s", compID, c.addr.String()),
		Session: c.sessionID,
	}
	_, _, err := kv.Acquire(pair, nil)
	if err != nil {
		log.Println("[coordinator] Error registering as publisher: ", err)
		return err
	}
	return nil
}

// DeregisterAsPublisher deregisters us as publishers of this stream and
func (c *consulCoordinator) DeregisterAsPublisher(compID StreamID) error {
	log.Println("[coordinator] Deregistering as publisher for: ", compID)
	kv := c.client.KV()
	key := fmt.Sprintf("dagger/publishers/%s/%s", compID, c.addr.String())
	_, err := kv.Delete(key, nil)
	if err != nil {
		log.Println("[coordinator] Error registering as publisher: ", err)
	}
	return nil
}

// ------------- OLD --------------

// JoinGroup joins the group that produces compID computation
func (c *consulCoordinator) JoinGroup(compID StreamID) (GroupHandler, error) {
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
	compID StreamID
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

// LEGACY ONLY, TO BE REMOVED

// GetSubscribers returns the addresses of subscribers interested in a certain topic
func (c *consulCoordinator) GetSubscribers(topic StreamID) ([]string, error) {
	tags := ParseTags(topic)
	log.Println("Publisher tags:", tags, topic)
	topic = StripTags(topic)
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

func (c *consulCoordinator) constructSubscriberKey(topic StreamID) string {
	return fmt.Sprintf("dagger/subscribers/%s/%s", topic, c.addr.String())
}

type subscribersList struct {
	subscribers []string
	prefix      string
	tags        Tags
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
	filtered := make([]string, 0, len(keys))
	for _, sub := range keys {
		if tagsMatch(sub, sl.tags) {
			idx := strings.LastIndex(sub, "/")
			if idx > 0 {
				filtered = append(filtered, sub[idx+1:])
			}
		}
	}
	sl.subscribers = filtered
	return nil
}

// WATCHES IMPLEMENTATIONS

type consulFunc func(*api.KV, string, uint64) (interface{}, *api.QueryMeta, error)

func (c *consulCoordinator) watch(key string, cf consulFunc, stop chan struct{}) (chan interface{}, chan error) {
	new := make(chan interface{})
	errc := make(chan error)
	go func() {
		kv := c.client.KV()
		var lastIndex uint64
		for {
			select {
			case <-stop:
				return
			default:
				// do a blocking query
				ret, queryMeta, err := cf(kv, key, lastIndex)
				log.Println("[consul] value updated at", key, ret)
				if err != nil {
					log.Println(err)
					errc <- err
					return
				}
				if ret != nil {
					new <- ret
				}
				lastIndex = queryMeta.LastIndex
			}
		}
	}()
	return new, errc
}

func consulGet(kv *api.KV, key string, lastIndex uint64) (interface{}, *api.QueryMeta, error) {
	return kv.Get(key, &api.QueryOptions{WaitIndex: lastIndex})
}

func consulKeys(kv *api.KV, prefix string, lastIndex uint64) (interface{}, *api.QueryMeta, error) {
	return kv.Keys(prefix, "", &api.QueryOptions{WaitIndex: lastIndex})
}

func (c *consulCoordinator) watchValue(key string, stop chan struct{}) (chan []byte, chan error) {
	new, errc := c.watch(key, consulGet, stop)
	vals := make(chan []byte)
	go func() {
		for {
			select {
			case <-stop:
				return
			case s := <-new:
				kvpair, ok := s.(*api.KVPair)
				if ok && kvpair != nil {
					vals <- kvpair.Value
				}
			}
		}
	}()
	return vals, errc
}

func (c *consulCoordinator) watchSet(prefix string, stop chan struct{}) (chan []string, chan error) {
	new, errc := c.watch(prefix, consulKeys, stop)
	sets := make(chan []string)
	go func() {
		for {
			select {
			case <-stop:
				return
			case s := <-new:
				keys := s.([]string)
				stripped := make([]string, len(keys), len(keys))
				for i := range keys {
					stripped[i] = keys[i][len(prefix):]
				}
				sets <- stripped
			}
		}
	}()
	return sets, errc
}

func (c *consulCoordinator) watchSetDiff(prefix string, stop chan struct{}) (chan string, chan string, chan error) {
	new, errc := c.watchSet(prefix, stop)
	added := make(chan string)
	dropped := make(chan string)
	go func() {
		var oldKeys []string
		for {
			select {
			case <-stop:
				return
			case newSet := <-new:
				// prepare a diff set so we can know which keys are newly
				// added and which are dropped
				diffSet := make(map[string]struct{})
				for _, s := range oldKeys {
					diffSet[s] = struct{}{}
				}
				oldKeys = make([]string, 0)
				for _, key := range newSet {
					_, exists := diffSet[key]
					if exists {
						delete(diffSet, key)
					} else {
						log.Println("[consul] new key ", key)
						added <- key
					}
					oldKeys = append(oldKeys, key)
				}
				for s := range diffSet {
					dropped <- s
				}
			}
		}
	}()
	return added, dropped, errc
}
