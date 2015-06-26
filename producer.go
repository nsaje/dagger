package main

import (
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"sync"

	"github.com/codegangsta/cli"
	"github.com/natefinch/pie"
)

func producer(c *cli.Context) {
	log.SetPrefix("[master log] ")
	log.Printf("starting main")
	prods := []string{
		"producer-test",
		"producer-test",
		"producer-test",
	}
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
				res, err := p.GetNext()
				if err != nil {
					log.Fatalf("error calling GetNext(): %s", err)
				}
				log.Printf("Response from plugin: %q", res)
			}
		}(path)
	}
	wg.Wait()
}

type producerPlugin struct {
	client *rpc.Client
}

func (p producerPlugin) GetNext() (result string, err error) {
	err = p.client.Call("Producer.GetNext", "", &result)
	return result, err
}
