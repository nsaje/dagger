package consul

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/hashicorp/consul/api"
	"github.com/nsaje/dagger/dagger"
)

const (
	// JobPrefix is the key prefix for tasks
	taskPrefix = "dagger/tasks/"
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

// NewCoordinator creates a new instance of Consul coordinator
func NewCoordinator(conf *api.Config) Coordinator {
	client, _ := api.NewClient(conf)
	c := &consulCoordinator{
		client: client,
		stopCh: make(chan struct{}),
		make(map[string]*subscribersList),
		new(sync.RWMutex),
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

type taskWatcher struct {
	*setWatcher
	new     chan dagger.Task
	dropped chan dagger.Task
}

func (c *consulCoordinator) NewTaskWatcher() dagger.TaskWatcher {
	w := &taskWatcher{
		setWatcher: c.newSetWatcher(taskPrefix),
		new:        make(chan dagger.Task),
		dropped:    make(chan dagger.Task),
	}
	go w.watch()
	return w
}

func (w *taskWatcher) watch() {
	for {
		select {
		case <-w.setWatcher.done:
			return
		case newJob := <-w.setWatcher.new:
			w.new <- dagger.Task(newJob[len(taskPrefix):])
		case droppedJob := <-w.setWatcher.dropped:
			w.dropped <- dagger.Task(droppedJob[len(taskPrefix):])
		}
	}
}

func (w *taskWatcher) New() chan dagger.Task {
	return w.new
}

func (w *taskWatcher) Dropped() chan dagger.Task {
	return w.dropped
}

func (c *consulCoordinator) AcquireTask(task dagger.Task) (bool, error) {
	log.Println("[coordinator] Trying to take task: ", task)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     string(task),
		Session: c.sessionID,
	}
	acquired, _, err := kv.Acquire(pair, nil)
	return acquired, err
}

func (c *consulCoordinator) TaskAcquired(task dagger.Task) (bool, error) {
	log.Println("[coordinator] Releasing task: ", task)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     string(task),
		Session: c.sessionID,
	}
	released, _, err := kv.Release(pair, nil)
	return released, err
}

func (c *consulCoordinator) ReleaseTask(task dagger.Task) (bool, error) {
	log.Println("[coordinator] Releasing task: ", task)
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     string(task),
		Session: c.sessionID,
	}
	released, _, err := kv.Release(pair, nil)
	return released, err
}

// setWatcher watches for and notifies of changes on a KV prefix
type setWatcher struct {
	new     chan string
	dropped chan string
	errc    chan error
	done    chan struct{}
}

func (c *consulCoordinator) newSetWatcher(prefix string) *setWatcher {
	w := &setWatcher{
		new:     make(chan string),
		dropped: make(chan string),
		errc:    make(chan error),
		done:    make(chan struct{}),
	}
	go w.watch(c.client.KV(), prefix)
	return w
}

func (w *setWatcher) New() chan string {
	return w.new
}

func (w *setWatcher) Dropped() chan string {
	return w.dropped
}

func (w *setWatcher) Error() chan error {
	return w.errc
}

func (w *setWatcher) Stop() {
	close(w.done)
}

func (w *setWatcher) watch(kv *api.KV, prefix string) {
	var lastIndex uint64
	var oldKeys []string
	for {
		select {
		case <-w.done:
			return
		default:
			// do a blocking query
			keys, queryMeta, err := kv.Keys(prefix, "",
				&api.QueryOptions{WaitIndex: lastIndex})
			log.Println("[consul] keys updated in ", prefix, keys)
			if err != nil {
				log.Println(err)
				w.errc <- err
				return
			}
			lastIndex = queryMeta.LastIndex

			// prepare a diff set so we can know which keys are newly
			// added and which are dropped
			diffSet := make(map[string]struct{})
			for _, s := range oldKeys {
				diffSet[s] = struct{}{}
			}
			log.Println("diffset", diffSet)
			oldKeys = make([]string, 0)
			for _, key := range keys {
				_, exists := diffSet[key]
				if exists {
					delete(diffSet, key)
				} else {
					log.Println("[consul] new key ", key)
					w.new <- key
				}
				oldKeys = append(oldKeys, key)
			}
			for s := range diffSet {
				w.dropped <- s
			}
		}
	}
}

// func (w *SetWatcher) watch(prefix string) {
// 	lastIndex := uint64(0)
// 	kv := c.client.KV()
// 	oldVal := ""
// 	go func() {
// 		for {
// 			select {
// 			case <-stopCh:
// 				return
// 			default:
// 				pair, queryMeta, err := kv.Get(key, &api.QueryOptions{WaitIndex: lastIndex})
// 				if err != nil {
// 					log.Println("[ERROR] consul watch", err) // FIXME
// 				}
// 				var newVal string
// 				if pair != nil {
// 					newVal = string(pair.Value)
// 				}
// 				log.Println("[watch] new val:", newVal)
// 				if newVal != oldVal {
// 					oldVal = newVal
// 					value <- newVal
// 				}
// 				lastIndex = queryMeta.LastIndex
// 			}
// 		}
// 	}()
// 	return value
// }

// ------------- OLD --------------

// // ManageJobs manages jobs
// func (c *consulCoordinator) ManageJobs(cm dagger.ComputationManager) {
// 	kv := c.client.KV()
// 	lastIndex := uint64(0)
// 	unapplicableSet := make(map[string]struct{})
// 	for {
// 		select {
// 		case <-c.stopCh:
// 			return
// 		default:
// 			keys, queryMeta, err := kv.Keys(JobPrefix, "", &api.QueryOptions{WaitIndex: lastIndex})
// 			log.Println("[coordinator][WatchJobs] Jobs checked ")
// 			if err != nil {
// 				// FIXME
// 				log.Println("[ERROR][coordinator][WatchJobs]:", err)
// 				panic(err)
// 			}
// 			lastIndex = queryMeta.LastIndex
//
// 			randomOrder := rand.Perm(len(keys))
// 			for _, i := range randomOrder {
// 				computationID := keys[i][len(JobPrefix):]
// 				if alreadyTaken := cm.Has(computationID); alreadyTaken {
// 					continue
// 				}
// 				if _, found := unapplicableSet[computationID]; found {
// 					continue
// 				}
// 				gotJob, err := c.TakeJob(keys[i])
// 				if err != nil {
// 					// FIXME
// 					log.Println("[ERROR][coordinator][WatchJobs][gotJob]:", err)
// 					panic(err)
// 				}
// 				if gotJob {
// 					log.Println("[coordinator] Got job:", keys[i])
// 					err = cm.SetupComputation(computationID)
// 					if err != nil {
// 						log.Println("Error setting up computation:", err) // FIXME
// 						c.ReleaseJob(keys[i])
// 						unapplicableSet[computationID] = struct{}{}
// 						continue
// 					}
// 					// job set up successfuly, register as publisher and delete the job
// 					log.Println("[coordinator] Deleting job: ", computationID)
// 					kv.Delete(keys[i], nil)
// 					c.RegisterAsPublisher(computationID)
// 				}
// 			}
// 		}
// 	}
// }
//
// // TakeJob tries to take a job from the job list. If another Worker
// // manages to take the job, the call returns false.
// func (c *consulCoordinator) TakeJob(job string) (bool, error) {
// 	log.Println("[coordinator] Trying to take job: ", job)
// 	kv := c.client.KV()
// 	pair := &api.KVPair{
// 		Key:     job,
// 		Session: c.sessionID,
// 	}
// 	acquired, _, err := kv.Acquire(pair, nil)
// 	return acquired, err
// }
//
// // ReleaseJob releases the job
// func (c *consulCoordinator) ReleaseJob(job string) (bool, error) {
// 	log.Println("[coordinator] Releasing job: ", job)
// 	kv := c.client.KV()
// 	pair := &api.KVPair{
// 		Key:     job,
// 		Session: c.sessionID,
// 	}
// 	released, _, err := kv.Release(pair, nil)
// 	return released, err
// }
//

// RegisterAsPublisher registers us as publishers of this stream and
func (c *consulCoordinator) RegisterAsPublisher(compID string) {
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
func (c *consulCoordinator) SubscribeTo(topic string, from time.Time) error {
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     c.constructSubscriberKey(topic),
		Value:   []byte(fmt.Sprintf("%d", from.UnixNano())),
		Session: c.sessionID,
	}
	// ignore bool, since if it's false, it just means we're already subscribed
	_, _, err := kv.Acquire(pair, nil)

	if strings.ContainsAny(topic, "()") {
		// only monitor publishers if it's a computation
		go c.monitorPublishers(topic)
	}
	return err
}

func (c *consulCoordinator) CheckpointPosition(topic string, from time.Time) error {
	kv := c.client.KV()
	pair := &api.KVPair{
		Key:     c.constructSubscriberKey(topic),
		Value:   []byte(fmt.Sprintf("%d", from.UnixNano())),
		Session: c.sessionID,
	}
	_, err := kv.Put(pair, nil)
	if err != nil {
		log.Println("[checkpoint] error", err)
	}
	return err
}

// UnsubscribeFrom unsubscribes from topics with global coordination
func (c *consulCoordinator) UnsubscribeFrom(topic string) error {
	kv := c.client.KV()
	key := c.constructSubscriberKey(topic)
	_, err := kv.Delete(key, nil)
	return err
}

// JoinGroup joins the group that produces compID computation
func (c *consulCoordinator) JoinGroup(compID string) (dagger.GroupHandler, error) {
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
	compID string
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
	key := fmt.Sprintf("dagger/%s/publishers_leader", gh.compID)
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

func (c *consulCoordinator) monitorPublishers(topic string) {
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
				Key: fmt.Sprintf("dagger/jobs/%s", topic),
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

func parseTags(topic string) dagger.Tags {
	tags := make(dagger.Tags)
	idx0 := strings.Index(topic, "{")
	idx1 := strings.Index(topic, "}")
	if idx0 == -1 || idx1 == -1 {
		return nil
	}
	taglist := topic[idx0+1 : idx1]
	pairs := strings.Split(taglist, ",")
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			continue
		}
		tags[kv[0]] = kv[1]
	}
	if len(tags) == 0 {
		return nil
	}
	return tags
}

func stripTags(topic string) string {
	idx0 := strings.Index(topic, "{")
	if idx0 > 0 {
		return topic[:idx0]
	}
	return topic
}

func (c *consulCoordinator) WatchSubscribers(topic string, stopCh chan struct{}) (new chan newSubscriber, dropped chan string) {
	tags := parseTags(topic)
	topic = stripTags(topic)
	prefix := fmt.Sprintf("dagger/subscribers/%s/", topic)

	new = make(chan newSubscriber)
	dropped = make(chan string)

	go func() {
		var lastIndex uint64
		var oldSubscribers []string
		kv := c.client.KV()
		// fmt.Println("Executing blocking consul.Keys method, lastIndex: ", sl.lastIndex)
		for {
			select {
			case <-stopCh:
				return
			default:
				// do a blocking query
				keys, queryMeta, err := kv.Keys(prefix, "", &api.QueryOptions{WaitIndex: lastIndex})
				log.Println("[coordinator] WatchSubscribers updated in ", prefix)
				// fmt.Println("consul.Keys method returned, New LastIndex: ", queryMeta.LastIndex)
				if err != nil {
					log.Println(err) // FIXME
					continue
				}
				lastIndex = queryMeta.LastIndex

				// prepare a diff set so we can know which subscribers are newly
				// subscribed and which are dropped
				diffSet := make(map[string]struct{})
				for _, s := range oldSubscribers {
					diffSet[s] = struct{}{}
				}
				oldSubscribers = make([]string, 0)
			SUBSCRIBER_LOOP:
				for _, key := range keys {
					subscriberTags := parseTags(key)
					log.Println("[coordinator][tags] Publisher", tags, ", subscriber", subscriberTags)
					for k, v := range subscriberTags {
						if tags[k] != v {
							continue SUBSCRIBER_LOOP
						}
					}
					if idx := strings.LastIndex(key, "/"); idx > 0 {
						subscriber := key[idx+1:]
						_, exists := diffSet[subscriber]
						if !exists {
							diffSet[subscriber] = struct{}{}
							log.Println("coordinator new subscriber", subscriber)
							pair, _, err := kv.Get(key, nil)
							if err != nil {
								panic(err)
							}
							from, err := strconv.ParseInt(string(pair.Value), 10, 64)
							if err != nil {
								panic(err)
							}
							new <- newSubscriber{subscriber, time.Unix(0, from)}
						}
						oldSubscribers = append(oldSubscribers, subscriber)
						delete(diffSet, subscriber)
					}
				}
				for s := range diffSet {
					dropped <- s
				}
			}
		}
	}()
	return
}

// GetSubscribers returns the addresses of subscribers interested in a certain topic
func (c *consulCoordinator) GetSubscribers(topic string) ([]string, error) {
	tags := parseTags(topic)
	log.Println("Publisher tags:", tags, topic)
	topic = stripTags(topic)
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

func (c *consulCoordinator) constructSubscriberKey(topic string) string {
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
		if time.Since(lastAccess) >= sl.c.config.SubscribersTTL {
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
	subscribers := make([]string, 0, len(keys))
	for _, key := range keys {
		tags := parseTags(key)
		log.Println("[coordinator][tags] Publisher", sl.tags, ", subscriber", tags)
		tagsMatch := true
		for k, v := range tags {
			if sl.tags[k] != v {
				tagsMatch = false
			}
		}
		if tagsMatch {
			idx := strings.LastIndex(key, "/")
			if idx > 0 {
				subscribers = append(subscribers, key[idx+1:])
			}
		}
	}
	sl.subscribers = subscribers
	return nil
}

func (c *consulCoordinator) WatchSubscriberPosition(topic string, subscriber string, stopCh chan struct{}, position chan time.Time) {
	key := fmt.Sprintf("dagger/subscribers/%s/%s", topic, subscriber)
	value := c.watch(key, stopCh, nil)
	for {
		select {
		case <-stopCh:
			return
		case v := <-value:
			posNsec, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				posNsec = 0
			}
			pos := time.Unix(0, posNsec)
			position <- pos
		}
	}
}

func (c *consulCoordinator) watch(key string, stopCh chan struct{}, value chan string) chan string {
	if value == nil {
		value = make(chan string)
	}
	lastIndex := uint64(0)
	// keys, queryMeta, err := kv.Keys(sl.prefix, "", &api.QueryOptions{WaitIndex: sl.lastIndex})
	kv := c.client.KV()
	oldVal := ""
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				pair, queryMeta, err := kv.Get(key, &api.QueryOptions{WaitIndex: lastIndex})
				if err != nil {
					log.Println("[ERROR] consul watch", err) // FIXME
				}
				var newVal string
				if pair != nil {
					newVal = string(pair.Value)
				}
				log.Println("[watch] new val:", newVal)
				if newVal != oldVal {
					oldVal = newVal
					value <- newVal
				}
				lastIndex = queryMeta.LastIndex
			}
		}
	}()
	return value
}
