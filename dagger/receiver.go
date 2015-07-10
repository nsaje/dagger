package dagger

import (
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"

	"bitbucket.org/nsaje/dagger/structs"
)

// Receiver receives new tuples via incoming RPC calls
type Receiver struct {
	conf     *Config
	incoming chan *structs.Tuple
	server   *rpc.Server
	listener net.Listener
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
	log.Printf("got call for Receiver.SubmitTuple() with tuple: %v", t)
	doneCh, errCh := t.ProcessingStarted()
	r.incoming <- t
	select {
	case <-doneCh:
		*reply = "ok"
	case err := <-errCh:
		*reply = err.Error()
	}
	return nil
}

// StartReceiving starts receiving incoming tuples over RPC
func (r *Receiver) StartReceiving() chan *structs.Tuple {
	go func() {
		for {
			if conn, err := r.listener.Accept(); err != nil {
				log.Fatal("accept error: " + err.Error())
			} else {
				log.Printf("new connection to the receiver established\n")
				go r.server.ServeCodec(jsonrpc.NewServerCodec(conn))
			}
		}
	}()
	return r.incoming
}
