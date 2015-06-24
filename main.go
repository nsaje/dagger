package main

import (
	"fmt"
	"os"

	"bitbucket.org/nsaje/dagger/producers"
	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/pub"
	"github.com/gdamore/mangos/transport/ipc"
	"github.com/gdamore/mangos/transport/tcp"
)

func die(format string, v ...interface{}) {
	fmt.Fprintln(os.Stderr, fmt.Sprintf(format, v...))
	os.Exit(1)
}

func main() {
	prods := []producers.Producer{
		&producers.TestProducerPlugin{},
		&producers.TestProducerPlugin{},
	}
	streams := make(chan producers.Stream)
	for _, p := range prods {
		go p.StartProducing(streams)
	}
	for {
		select {
		case newStream, ok := <-streams:
			if !ok {
				panic("what?")
			}
			go func(stream producers.Stream) {
				sock := newStreamSocket()
				for {
					val := <-stream
					fmt.Printf("%v\n", val)
					sock.Send()
				}
			}(newStream)
		}
	}
}

func newStreamSocket() mangos.Socket {
	var sock mangos.Socket
	var err error
	if sock, err = pub.NewSocket(); err != nil {
		die("can't get new pub socket: %s", err)
	}
	sock.AddTransport(ipc.NewTransport())
	sock.AddTransport(tcp.NewTransport())
	if err = sock.Listen("0.0.0.0:0"); err != nil {
		die("can't listen on pub socket: %s", err.Error())
	}
	return sock
}
