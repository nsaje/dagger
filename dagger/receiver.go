package dagger

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"

	"github.com/nsaje/dagger/structs"
)

// Receiver receives new tuples via incoming RPC calls
type Receiver struct {
	conf              *Config
	incoming          chan *structs.Tuple
	server            *rpc.Server
	listener          net.Listener
	next              TupleProcessor
	computationSyncer ComputationSyncer
}

// NewReceiver initializes a new receiver
func NewReceiver(conf *Config) *Receiver {
	r := &Receiver{conf: conf, incoming: make(chan *structs.Tuple)}
	r.server = rpc.NewServer()
	r.server.Register(r)
	r.server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	var err error
	r.listener, err = net.Listen("tcp", r.conf.RPCAdvertise.String())
	if err != nil {
		log.Fatal("listen error:", err)
	}
	return r
}

// ListenAddr is the network address on which the receiver listens
func (r *Receiver) ListenAddr() net.Addr {
	return r.listener.Addr()
}

// SubmitTuple submits a new tuple into the worker process
func (r *Receiver) SubmitTuple(t *structs.Tuple, reply *string) error {
	log.Printf("[receiver] tuple: %v", t)
	err := r.next.ProcessTuple(t)
	if err != nil {
		log.Println(err)
		return err
	}
	*reply = "ok"
	return nil
}

// Sync is the RPC method called by slave workers wanting to sync a computation
func (r *Receiver) Sync(compID string, reply *structs.ComputationSnapshot) error {
	log.Printf("[receiver] sync request for %s", compID)
	if r.computationSyncer == nil {
		return fmt.Errorf("Computation manager doesn't exist!")
	}
	snapshot, err := r.computationSyncer.Sync(compID)
	log.Printf("[receiver] snapshot: %v, err: %v", snapshot, err)
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
func (r *Receiver) ReceiveTuples(next TupleProcessor) {
	r.next = next
	for {
		if conn, err := r.listener.Accept(); err != nil {
			log.Fatal("accept error: " + err.Error())
		} else {
			log.Printf("new connection to the receiver established\n")
			go r.server.ServeCodec(jsonrpc.NewServerCodec(conn))
		}
	}
}
