package command

import (
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"sync"

	"github.com/nsaje/dagger/dagger"
	"github.com/nsaje/dagger/structs"

	"github.com/codegangsta/cli"
	"github.com/natefinch/pie"
	"github.com/twinj/uuid"
)

// Producer collects data from a system / messaging queue / ... and submits it
// to registered subscribers via RPC
func Producer(c *cli.Context) {
	log.SetPrefix("[master log] ")
	log.Printf("starting main")
	// TODO: discover all (enabled) producers
	prods := []string{
		"./producer-test",
		// "producer-test",
		// "producer-test",
	}
	conf := dagger.DefaultConfig()
	coordinator := dagger.NewCoordinator(conf, conf.RPCAdvertise)
	err := coordinator.Start()
	defer coordinator.Stop()
	if err != nil {
		log.Fatal("error setting up coordinator")
	}

	coordinator.RegisterAsPublisher("test")

	dispatcher := dagger.NewDispatcher(conf, coordinator)

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
				dispatcher.ProcessTuple(res)
			}
		}(path)
	}
	// wg.Wait()
	handleSignals()
}

type producerPlugin struct {
	client *rpc.Client
}

func (p producerPlugin) GetNext() (*structs.Tuple, error) {
	var result structs.Tuple
	err := p.client.Call("Producer.GetNext", "", &result)
	// assign ID
	result.ID = uuid.NewV4().String()
	return &result, err
}

func handleMessage(msg string) {
	// log.Printf("Response from plugin: %q", msg)
}
