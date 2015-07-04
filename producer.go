package main

import (
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"sync"

	"bitbucket.org/nsaje/dagger/structs"

	"github.com/codegangsta/cli"
	"github.com/natefinch/pie"
)

func producer(c *cli.Context) {
	log.SetPrefix("[master log] ")
	log.Printf("starting main")
	// TODO: discover all (enabled) producers
	prods := []string{
		"./producer-test",
		// "producer-test",
		// "producer-test",
	}
	output := make(chan structs.Tuple)
	conf := DefaultConfig()
	coordinator, err := newCoordinator(conf)
	if err != nil {
		log.Fatal("error setting up coordinator")
	}
	go startDispatching(conf, coordinator, output)

	var wg sync.WaitGroup
	for _, path := range prods {
		log.Printf("handling producer %s", path)
		wg.Add(1)
		go func(pluginPath string) {
			defer wg.Done()
			log.Printf("launching producer")
			client, err := pie.StartProviderCodec(jsonrpc.NewClientCodec, os.Stderr, pluginPath)
			if err != nil {
				log.Fatalf("Error running plugin: %s", err)
			}
			defer client.Close()
			p := producerPlugin{client}
			for {
				log.Println("calling getnext")
				res, err := p.GetNext()
				log.Println("getnext returned")
				if err != nil {
					log.Fatalf("error calling GetNext(): %s", err)
				}
				// handleMessage(res)
				output <- res
			}
		}(path)
	}
	wg.Wait()
}

type producerPlugin struct {
	client *rpc.Client
}

func (p producerPlugin) GetNext() (result structs.Tuple, err error) {
	err = p.client.Call("Producer.GetNext", "", &result)
	return result, err
}

func handleMessage(msg string) {
	// log.Printf("Response from plugin: %q", msg)
}
