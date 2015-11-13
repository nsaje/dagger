package dagger

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"

	"github.com/nsaje/dagger/s"
)

// Receiver receives new tuples via incoming RPC calls
type Receiver struct {
	conf                      *Config
	coordinator               Coordinator
	server                    *rpc.Server
	listener                  net.Listener
	taskManager               *TaskManager
	subscribedTupleProcessors map[s.StreamID]map[TupleProcessor]struct{}
	checkpointTimers          map[s.StreamID]*time.Timer
	subscribersLock           *sync.RWMutex
}

// NewReceiver initializes a new receiver
func NewReceiver(conf *Config, coordinator Coordinator) *Receiver {
	r := &Receiver{
		conf:                      conf,
		coordinator:               coordinator,
		subscribedTupleProcessors: make(map[s.StreamID]map[TupleProcessor]struct{}),
		checkpointTimers:          make(map[s.StreamID]*time.Timer),
		subscribersLock:           &sync.RWMutex{},
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
func (r *Receiver) ListenAddr() net.Addr {
	return r.listener.Addr()
}

func (r *Receiver) SubscribeTo(streamID s.StreamID, from s.Timestamp, tp TupleProcessor) {
	r.subscribersLock.Lock()
	defer r.subscribersLock.Unlock()
	r.coordinator.SubscribeTo(streamID, from)
	subscribersSet := r.subscribedTupleProcessors[streamID]
	if subscribersSet == nil {
		subscribersSet = make(map[TupleProcessor]struct{})
		r.checkpointTimers[streamID] = time.NewTimer(time.Second) // FIXME: configurable
	}
	subscribersSet[tp] = struct{}{}
	r.subscribedTupleProcessors[streamID] = subscribersSet
}

func (r *Receiver) UnsubscribeFrom(streamID s.StreamID, tp TupleProcessor) {
	r.subscribersLock.Lock()
	defer r.subscribersLock.Unlock()
	r.coordinator.UnsubscribeFrom(streamID)
	delete(r.subscribedTupleProcessors[streamID], tp) // FIXME: delete streamID from all maps when empty
}

// SubmitTuple submits a new tuple into the worker process
func (r *Receiver) SubmitTuple(t *s.Tuple, reply *string) error {
	r.subscribersLock.RLock()
	defer r.subscribersLock.RUnlock()
	log.Printf("[receiver] Received: %s", t)
	// if rand.Float64() < 0.1 {
	// 	fmt.Println("rejecting for test")
	// 	time.Sleep(time.Second)
	// 	return errors.New("rejected for test")
	// }
	subscribers := make([]TupleProcessor, 0, len(r.subscribedTupleProcessors[t.StreamID]))
	for k := range r.subscribedTupleProcessors[t.StreamID] {
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
func (r *Receiver) Sync(compID s.StreamID, reply *[]byte) error {
	log.Printf("[receiver] Sync request for %s", compID)
	if r.taskManager == nil {
		return fmt.Errorf("[receiver] Task manager doesn't exist")
	}
	snapshot, err := r.taskManager.GetSnapshot(compID)
	log.Printf("[receiver] Replying with snapshot: %v, err: %v", snapshot, err)
	if err != nil {
		return err
	}
	*reply = snapshot
	return err
}

// SetTaskManager sets the task manager that is forwarded task sync requests
func (r *Receiver) SetTaskManager(tm *TaskManager) {
	r.taskManager = tm
}

// Listen starts receiving incoming tuples over RPC
func (r *Receiver) Listen() {
	for {
		if conn, err := r.listener.Accept(); err != nil {
			log.Fatal("[receiver] Accept error: " + err.Error())
		} else {
			log.Printf("[receiver] New connection to the receiver established\n")
			go r.server.ServeCodec(jsonrpc.NewServerCodec(conn))
		}
	}
}
