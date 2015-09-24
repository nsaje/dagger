package command

import (
	"bufio"
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"strings"
	"sync"
	"time"

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
	// TODO: discover all (enabled) producers
	// prods := []string{
	// 	"./producer-test_stdin",
	// 	// "producer-test",
	// 	// "producer-test",
	// }
	prods := []string{c.Args().First()}
	conf := dagger.DefaultConfig()
	coordinator := dagger.NewCoordinator(conf, conf.RPCAdvertise)
	err := coordinator.Start()
	defer coordinator.Stop()
	if err != nil {
		log.Fatal("Error setting up coordinator")
	}

	coordinator.RegisterAsPublisher(c.Args().Get(1))

	dispatcher := dagger.NewDispatcher(conf, coordinator)

	var wg sync.WaitGroup
	for _, path := range prods {
		wg.Add(1)
		go func(pluginPath string) {
			defer wg.Done()
			if c.Args().First() == "producer-stdin" {
				for {
					reader := bufio.NewReader(os.Stdin)
					var line string
					line, _ = reader.ReadString('\n')
					tuple := &structs.Tuple{
						ID:       uuid.NewV4().String(),
						StreamID: c.Args().Get(1),
						LWM:      time.Now(),
						Data:     strings.TrimSpace(line),
					}
					dispatcher.ProcessTuple(tuple)
				}
			} else {
				log.Printf("Launching producer")
				client, err := pie.StartProviderCodec(jsonrpc.NewClientCodec, os.Stderr, pluginPath)
				if err != nil {
					log.Fatalf("Error running plugin: %s", err)
				}
				defer client.Close()
				p := producerPlugin{client}
				for {
					log.Println("Getting next tuple from plugin...")
					res, err := p.GetNext()
					if err != nil {
						log.Fatalf("Error calling GetNext(): %s", err)
					}
					dispatcher.ProcessTuple(res)
				}
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
