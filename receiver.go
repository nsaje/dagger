package main

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
}

// SubmitTuple submits a new tuple into the worker process
func (r *Receiver) SubmitTuple(t *structs.Tuple, reply *string) error {
	log.Printf("got call for Receiver.SubmitTuple() with tuple: %v", t)
	r.incoming <- t
	*reply = "ok"
	return nil
}

func startReceiving(conf *Config, inc chan *structs.Tuple) {
	l := &Receiver{conf, inc}
	server := rpc.NewServer()
	server.Register(l)
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	listener, err := net.Listen("tcp", conf.RPCAdvertise.String())
	if err != nil {
		log.Fatal("listen error:", err)
	}
	for {
		if conn, err := listener.Accept(); err != nil {
			log.Fatal("accept error: " + err.Error())
		} else {
			log.Printf("new connection to the receiver established\n")
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
		}
	}
}
