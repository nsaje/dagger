package dagger

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

const (
	// JobPrefix is the key prefix for jobs
	JobPrefix = "dagger/jobs/"
)

// Coordinator coordinates topics, their publishers and subscribers
type Coordinator interface {
	// GetSubscribers(string) ([]string, error)
	// GetConfig(string) (string, error)
	SubscribeCoordinator
	PublishCoordinator
	JobCoordinator
	ReplicationCoordinator
	Start() error
	Stop()
}

// SubscribeCoordinator handles the act of subscribing to a stream
type SubscribeCoordinator interface {
	SubscribeTo(streamID string) error
}

// PublishCoordinator handles the coordination of publishing a stream
type PublishCoordinator interface {
	GetSubscribers(streamID string) ([]string, error)
	RegisterAsPublisher(streamID string)
}

// JobCoordinator coordinates accepts, starts and stops jobs
type JobCoordinator interface {
	ManageJobs(ComputationManager)
}

// ReplicationCoordinator coordinates replication of tuples onto multiple
// computations on multiple hosts for high availability
type ReplicationCoordinator interface {
	JoinGroup(streamID string) (GroupHandler, error)
}

// ConsulCoordinator implementation based on Consul.io
type ConsulCoordinator struct {
	client *api.Client
	config *Config
	addr   net.Addr

	sessionID    string
	sessionRenew chan struct{}
	stopCh       chan struct{}

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
		stopCh:      make(chan struct{}),
	}
	return c
}

// Start creates a new session and starts synchronizing state
func (c *ConsulCoordinator) Start() error {
	session := c.client.Session()
	// set session to delete our keys on invalidation
	sessionOptions := &api.SessionEntry{
		Behavior:  api.SessionBehaviorDelete,
		LockDelay: 100 * time.Millisecond,
	}
	sessionID, _, err := session.Create(sessionOptions, nil)
	if err != nil {
		return fmt.Errorf("failed to create consul session: %v", err)
	}

	// set up a long-running goroutine for renewing the session
	c.sessionRenew = make(chan struct{})
	c.sessionID = sessionID
	go session.RenewPeriodic(api.DefaultLockSessionTTL, sessionID, nil, c.sessionRenew)

	log.Print("coordinator ready")

	return nil
}

// Stop destroys the Consul session, deleting all our keys
func (c *ConsulCoordinator) Stop() {
	close(c.sessionRenew)
	c.sessionRenew = nil
	close(c.stopCh)

	session := c.client.Session()
	log.Printf("Destroying session %s", c.sessionID)
	session.Destroy(c.sessionID, nil) // ignoring error, session will expire anyway
}

// ManageJobs manages jobs
func (c *ConsulCoordinator) ManageJobs(cm ComputationManager) {
	kv := c.client.KV()
	lastIndex := uint64(0)
	unapplicableSet := make(map[string]struct{})
	for {
		select {
		case <-c.stopCh:
			return
		default:
			keys, queryMeta, err := kv.Keys(JobPrefix, "", &api.QueryOptions{WaitIndex: lastIndex})
			log.Println("[coordinator][WatchJobs] jobs checked ")
			if err != nil {
				// FIXME
				log.Println("ERROR managejobs:", err)
			}
			lastIndex = queryMeta.LastIndex

			randomOrder := rand.Perm(len(keys))
			for _, i := range randomOrder {
				computationID := keys[i][len(JobPrefix):]
				if alreadyTaken := cm.Has(computationID); alreadyTaken {
					continue
				}
				if _, found := unapplicableSet[computationID]; found {
					continue
				}
				gotJob, err := c.TakeJob(keys[i])
				if err != nil {
					log.Println(err) // FIXME
					continue
				}
				if gotJob {
					log.Println("[coordinator] got job: ", keys[i])
					err = cm.SetupComputation(computationID)
					if err != nil {
						log.Println("error setting up computation:", err) // FIXME
						c.ReleaseJob(keys[i])
						unapplicableSet[computationID] = struct{}{}
						continue
					}
					// job set up successfuly, register as publisher and delete the job
					log.Println("[coordinator] deleting job: ", computationID)
					kv.Delete(keys[i], nil)
					c.RegisterAsPublisher(computationID)
				}
			}
		}
	}
}

// TakeJob tries to take a job from the job list. If another Worker
// manages to take the job, the call returns false.
func (c *ConsulCoordinator) TakeJob(job string) (bool, error) {
	log.Println("[coordinator] trying to take job: ", job)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     job,
		Session: c.sessionID,
	}
	acquired, _, err := kv.Acquire(pair, nil)
	return acquired, err
}

// ReleaseJob releases the job
func (c *ConsulCoordinator) ReleaseJob(job string) (bool, error) {
	log.Println("[coordinator] releasing job: ", job)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     job,
		Session: c.sessionID,
	}
	released, _, err := kv.Release(pair, nil)
	return released, err
}

// RegisterAsPublisher registers us as publishers of this stream and
func (c *ConsulCoordinator) RegisterAsPublisher(compID string) {
	log.Println("[coordinator] registering as publisher for: ", compID)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     fmt.Sprintf("dagger/%s/publishers/%s", compID, c.addr.String()),
		Session: c.sessionID,
	}
	_, _, err := kv.Acquire(pair, nil)
	if err != nil {
		log.Println(err)
	}
}

// SubscribeTo subscribes to topics with global coordination
func (c *ConsulCoordinator) SubscribeTo(topic string) error {
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     c.constructSubscriberKey(topic),
		Session: c.sessionID,
	}
	// ignore bool, since if it's false, it just means we're already subscribed
	_, _, err := kv.Acquire(pair, nil)
	go c.monitorPublishers(topic)
	return err
}

// JoinGroup joins the group that produces compID computation
func (c *ConsulCoordinator) JoinGroup(compID string) (GroupHandler, error) {
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

// GroupHandler handles leadership status of a group
type GroupHandler interface {
	GetStatus() (bool, string, error)
}

type groupHandler struct {
	compID string
	c      *ConsulCoordinator

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
	key := fmt.Sprintf("dagger/%s/publishers_leader", gh.compID)
	if gh.currentLeader == "" {
		pair := &api.KVPair{
			Key:     key,
			Session: gh.c.sessionID,
			Value:   []byte(gh.c.addr.String()),
		}
		kv := gh.c.client.KV()
		log.Println("[coordinator][groupHandler] trying to acquire leadership of ", gh.compID)
		_, _, err := kv.Acquire(pair, nil)
		if err != nil {
			log.Println("error acquiring leadership", err)
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
	log.Println("[coordinator][groupHandler] fetch returned new data")
	if err != nil {
		log.Println("FETCH ERROR")
		return err
	}
	gh.Lock()
	gh.lastIndex = queryMeta.LastIndex
	log.Println("leader keypair:", pair)
	if pair == nil || pair.Session == "" {
		log.Println("in branch 1")
		gh.currentLeader = ""
		gh.areWeLeader = false
	} else {
		log.Println("in branch 2")
		gh.currentLeader = string(pair.Value)
		gh.areWeLeader = (gh.currentLeader == gh.c.addr.String())
	}
	gh.Unlock()

	log.Println("NEW LEADER: ", gh.currentLeader, pair)

	return nil
}

func (c *ConsulCoordinator) monitorPublishers(topic string) {
	prefix := fmt.Sprintf("dagger/%s/publishers/", topic)
	kv := c.client.KV()
	lastIndex := uint64(0)
	lastNumPublishers := -1
	for {
		keys, queryMeta, err := kv.Keys(prefix, "", &api.QueryOptions{WaitIndex: lastIndex})
		log.Println("[coordinator] publishers checked in ", prefix)
		if err != nil {
			log.Println("ERROR:", err)
			// FIXME
		}
		log.Println("last index before, after ", lastIndex, queryMeta.LastIndex)
		lastIndex = queryMeta.LastIndex

		// if there are no publishers registered, post a new job
		if len(keys) != lastNumPublishers && len(keys) < 2 {
			log.Printf("Number of publishers of %s is %d, posting job.", topic, len(keys))
			log.Println("Publishers: ", keys)
			pair := &api.KVPair{
				Key: fmt.Sprintf("dagger/jobs/%s", topic),
			}
			kv.Put(pair, nil) // FIXME error handling
		} else { // FIXME: do this more elegantly
			if len(keys) == 2 {
				log.Printf("Number of publishers of %s is %d, not posting job.", topic, len(keys))
				log.Println("Publishers: ", keys)
			}
		}
		lastNumPublishers = len(keys)
	}
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
	// fmt.Println("Executing blocking consul.Keys method, lastIndex: ", sl.lastIndex)
	keys, queryMeta, err := kv.Keys(sl.prefix, "", &api.QueryOptions{WaitIndex: sl.lastIndex})
	log.Println("[coordinator] subscribers updated in ", sl.prefix)
	// fmt.Println("consul.Keys method returned, New LastIndex: ", queryMeta.LastIndex)
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
