package dagger

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strings"
	"sync"
	"time"
)

// ReceiverConfig configures the RPC receiver
type ReceiverConfig struct {
	Addr string
	Port string
}

func defaultReceiverConfig() *ReceiverConfig {
	return &ReceiverConfig{
		Addr: "0.0.0.0",
		Port: "46632",
	}
}

// Receiver receives new records via incoming RPC calls
type Receiver interface {
	InputManager
	ListenAddr() net.Addr
	Listen(chan error)
}

// InputManager manages tasks' subscriptions to streams
type InputManager interface {
	SubscribeTo(StreamID, Timestamp, RecordProcessor)
	UnsubscribeFrom(StreamID, RecordProcessor)
	SetTaskManager(TaskManager)
}

type receiver struct {
	conf                       *ReceiverConfig
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
func NewReceiver(coordinator Coordinator, customizeConfig func(*ReceiverConfig)) Receiver {
	conf := defaultReceiverConfig()
	if customizeConfig != nil {
		customizeConfig(conf)
	}
	r := &receiver{
		conf:                       conf,
		coordinator:                coordinator,
		subscribedRecordProcessors: make(map[StreamID]map[RecordProcessor]struct{}),
		publisherMonitors:          make(map[StreamID]chan struct{}),
		checkpointTimers:           make(map[StreamID]*time.Timer),
		subscribersLock:            &sync.RWMutex{},
	}
	r.server = rpc.NewServer()
	rpcHandler := &RPCHandler{r}
	r.server.Register(rpcHandler)
	r.server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	var err error
	r.listener, err = net.Listen("tcp", net.JoinHostPort(conf.Addr, conf.Port))
	if err != nil {
		log.Fatal("[receiver] Listen error:", err)
	}
	return r
}

// ListenAddr is the network address on which the receiver listens
func (r *receiver) ListenAddr() net.Addr {
	log.Printf("LISTENING ON ", r.listener.Addr())
	return r.listener.Addr()
}

// SetTaskManager sets the task manager that is forwarded task sync requests
func (r *receiver) SetTaskManager(tm TaskManager) {
	r.taskManager = tm
}

// Listen starts receiving incoming records over RPC
func (r *receiver) Listen(errc chan error) {
	for {
		if conn, err := r.listener.Accept(); err != nil {
			log.Printf("[receiver] Accept error: " + err.Error())
			errc <- err
		} else {
			log.Printf("[receiver] New connection to the receiver established\n")
			go r.server.ServeCodec(jsonrpc.NewServerCodec(conn))
		}
	}
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
			stopCh := r.publisherMonitors[streamID]
			if stopCh != nil {
				close(stopCh)
			}
			delete(r.publisherMonitors, streamID)
		}
	}
}

// RPCHandler handles incoming RPC calls
type RPCHandler struct {
	r *receiver
}

// SubmitRecord submits a new record into the worker process
func (rpci *RPCHandler) SubmitRecord(t *Record, reply *string) error {
	rpci.r.subscribersLock.RLock()
	defer rpci.r.subscribersLock.RUnlock()
	log.Printf("[receiver] Received: %s", t)
	subscribers := make([]RecordProcessor, 0, len(rpci.r.subscribedRecordProcessors[t.StreamID]))
	for k := range rpci.r.subscribedRecordProcessors[t.StreamID] {
		subscribers = append(subscribers, k)
	}
	err := ProcessMultipleProcessors(subscribers, t)
	if err != nil {
		log.Printf("[ERROR] Processing %s failed: %s", t, err)
		return err
	}
	// if enough time has passed, create a checkpoint in coordination system
	select {
	case <-rpci.r.checkpointTimers[t.StreamID].C:
		log.Printf("[receiver] checkpointing position of stream %s at %s", t.StreamID, t.Timestamp)
		rpci.r.coordinator.CheckpointPosition(t.StreamID, t.Timestamp)
		rpci.r.checkpointTimers[t.StreamID].Reset(time.Second)
	default:
	}
	*reply = "ok"
	log.Printf("[receiver] ACKed: %s", t)
	return nil
}

// Sync is the RPC method called by slave workers wanting to sync a computation
func (rpci *RPCHandler) Sync(compID StreamID, reply *[]byte) error {
	log.Printf("[receiver] Sync request for %s", compID)
	if rpci.r.taskManager == nil {
		return fmt.Errorf("[receiver] Task manager doesn't exist")
	}
	snapshot, err := rpci.r.taskManager.GetTaskSnapshot(compID)
	if err != nil {
		return err
	}
	*reply = snapshot
	return err
}
