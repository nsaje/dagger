package dagger

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strings"
	"sync"
	"time"
)

// Receiver receives new records via incoming RPC calls
// and manages subscriptions to streams
type Receiver interface {
	ListenAddr() net.Addr
	SubscribeTo(StreamID, Timestamp, RecordProcessor)
	UnsubscribeFrom(StreamID, RecordProcessor)
	SetTaskManager(TaskManager)
	Listen()
}

type receiver struct {
	conf                       *Config
	coordinator                Coordinator
	server                     *rpc.Server
	listener                   net.Listener
	taskManager                TaskManager
	subscribedRecordProcessors map[StreamID]map[RecordProcessor]struct{}
	publisherMonitors          map[StreamID]chan struct{}
	checkpointTimers           map[StreamID]*time.Timer
	subscribersLock            *sync.RWMutex
}

// NewReceiver initializes a new receiver
func NewReceiver(conf *Config, coordinator Coordinator) Receiver {
	r := &receiver{
		conf:                       conf,
		coordinator:                coordinator,
		subscribedRecordProcessors: make(map[StreamID]map[RecordProcessor]struct{}),
		publisherMonitors:          make(map[StreamID]chan struct{}),
		checkpointTimers:           make(map[StreamID]*time.Timer),
		subscribersLock:            &sync.RWMutex{},
	}
	r.server = rpc.NewServer()
	r.server.Register(r)
	r.server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	var err error
	r.listener, err = net.Listen("tcp", r.conf.RPCAdvertise.String())
	if err != nil {
		log.Fatal("[receiver] Listen error:", err)
	}
	return r
}

// ListenAddr is the network address on which the receiver listens
func (r *receiver) ListenAddr() net.Addr {
	return r.listener.Addr()
}

func (r *receiver) SubscribeTo(streamID StreamID, from Timestamp, tp RecordProcessor) {
	r.subscribersLock.Lock()
	defer r.subscribersLock.Unlock()
	subscribersSet := r.subscribedRecordProcessors[streamID]
	if subscribersSet == nil {
		r.coordinator.SubscribeTo(streamID, from) // FIXME: return error
		if strings.ContainsAny(string(streamID), "()") {
			// only monitor publishers if it's a computation
			stop := make(chan struct{})
			r.coordinator.EnsurePublisherNum(streamID, 2, stop)
			r.publisherMonitors[streamID] = stop
		}
		subscribersSet = make(map[RecordProcessor]struct{})
		r.checkpointTimers[streamID] = time.NewTimer(time.Second) // FIXME: configurable
	}
	subscribersSet[tp] = struct{}{}
	r.subscribedRecordProcessors[streamID] = subscribersSet
}

func (r *receiver) UnsubscribeFrom(streamID StreamID, rp RecordProcessor) {
	r.subscribersLock.Lock()
	defer r.subscribersLock.Unlock()
	r.coordinator.UnsubscribeFrom(streamID)
	subProcs := r.subscribedRecordProcessors[streamID]
	if subProcs != nil {
		delete(subProcs, rp)
		if len(subProcs) == 0 {
			delete(r.subscribedRecordProcessors, streamID)
			close(r.publisherMonitors[streamID])
			delete(r.publisherMonitors, streamID)
		}
	}
}

// SubmitRecord submits a new record into the worker process
func (r *receiver) SubmitRecord(t *Record, reply *string) error {
	r.subscribersLock.RLock()
	defer r.subscribersLock.RUnlock()
	log.Printf("[receiver] Received: %s", t)
	// if rand.Float64() < 0.1 {
	// 	fmt.Println("rejecting for test")
	// 	time.Sleep(time.Second)
	// 	return errors.New("rejected for test")
	// }
	subscribers := make([]RecordProcessor, 0, len(r.subscribedRecordProcessors[t.StreamID]))
	for k := range r.subscribedRecordProcessors[t.StreamID] {
		subscribers = append(subscribers, k)
	}
	// log.Printf("[receiver] processing %s with %+v", t, subscribers)
	err := ProcessMultipleProcessors(subscribers, t)
	if err != nil {
		log.Printf("[ERROR] Processing %s failed: %s", t, err)
		return err
	}
	// if enough time has passed, create a checkpoint in coordination system
	select {
	case <-r.checkpointTimers[t.StreamID].C:
		log.Printf("[receiver] checkpointing position of stream %s at %s", t.StreamID, t.Timestamp)
		r.coordinator.CheckpointPosition(t.StreamID, t.Timestamp)
		r.checkpointTimers[t.StreamID].Reset(time.Second)
	default:
	}
	*reply = "ok"
	log.Printf("[receiver] ACKed: %s", t)
	return nil
}

// Sync is the RPC method called by slave workers wanting to sync a computation
func (r *receiver) Sync(compID StreamID, reply *[]byte) error {
	log.Printf("[receiver] Sync request for %s", compID)
	if r.taskManager == nil {
		return fmt.Errorf("[receiver] Task manager doesn't exist")
	}
	snapshot, err := r.taskManager.GetTaskSnapshot(compID)
	if err != nil {
		return err
	}
	*reply = snapshot
	return err
}

// SetTaskManager sets the task manager that is forwarded task sync requests
func (r *receiver) SetTaskManager(tm TaskManager) {
	r.taskManager = tm
}

// Listen starts receiving incoming records over RPC
func (r *receiver) Listen() {
	for {
		if conn, err := r.listener.Accept(); err != nil {
			log.Fatal("[receiver] Accept error: " + err.Error())
		} else {
			log.Printf("[receiver] New connection to the receiver established\n")
			go r.server.ServeCodec(jsonrpc.NewServerCodec(conn))
		}
	}
}
