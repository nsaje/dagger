package dagger

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"

	"github.com/nsaje/dagger/structs"
)

// Receiver receives new tuples via incoming RPC calls
type Receiver struct {
	conf                      *Config
	coordinator               Coordinator
	server                    *rpc.Server
	listener                  net.Listener
	computationSyncer         ComputationSyncer
	subscribedTupleProcessors map[string]map[TupleProcessor]struct{}
	subscribersLock           *sync.RWMutex
}

// NewReceiver initializes a new receiver
func NewReceiver(conf *Config, coordinator Coordinator) *Receiver {
	r := &Receiver{
		conf:                      conf,
		coordinator:               coordinator,
		subscribedTupleProcessors: make(map[string]map[TupleProcessor]struct{}),
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
	coordinator.SetAddr(r.listener.Addr())
	return r
}

// ListenAddr is the network address on which the receiver listens
func (r *Receiver) ListenAddr() net.Addr {
	return r.listener.Addr()
}

func (r *Receiver) SubscribeTo(streamID string, tp TupleProcessor) {
	r.subscribersLock.Lock()
	defer r.subscribersLock.Unlock()
	r.coordinator.SubscribeTo(streamID)
	subscribersSet := r.subscribedTupleProcessors[streamID]
	if subscribersSet == nil {
		subscribersSet = make(map[TupleProcessor]struct{})
	}
	subscribersSet[tp] = struct{}{}
	r.subscribedTupleProcessors[streamID] = subscribersSet
}

func (r *Receiver) UnsubscribeFrom(streamID string, tp TupleProcessor) {
	r.subscribersLock.Lock()
	defer r.subscribersLock.Unlock()
	r.coordinator.UnsubscribeFrom(streamID)
	delete(r.subscribedTupleProcessors[streamID], tp)
}

// SubmitTuple submits a new tuple into the worker process
func (r *Receiver) SubmitTuple(t *structs.Tuple, reply *string) error {
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
	*reply = "ok"
	log.Printf("[receiver] ACKed: %s", t)
	return nil
}

// Sync is the RPC method called by slave workers wanting to sync a computation
func (r *Receiver) Sync(compID string, reply *structs.ComputationSnapshot) error {
	log.Printf("[receiver] Sync request for %s", compID)
	if r.computationSyncer == nil {
		return fmt.Errorf("[receiver] Computation manager doesn't exist!")
	}
	snapshot, err := r.computationSyncer.Sync(compID)
	log.Printf("[receiver] Replying with snapshot: %v, err: %v", snapshot, err)
	if err != nil {
		return err
	}
	*reply = *snapshot
	return err
}

func (r *Receiver) SetComputationSyncer(cs ComputationSyncer) {
	r.computationSyncer = cs
}

// ReceiveTuples starts receiving incoming tuples over RPC
func (r *Receiver) ReceiveTuples() {
	for {
		if conn, err := r.listener.Accept(); err != nil {
			log.Fatal("[receiver] Accept error: " + err.Error())
		} else {
			log.Printf("[receiver] New connection to the receiver established\n")
			go r.server.ServeCodec(jsonrpc.NewServerCodec(conn))
		}
	}
}
