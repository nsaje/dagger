package command

import (
	"bufio"
	"log"
	"os"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/nsaje/dagger/dagger"
)

// Producer reads records from stdin and submits them
// to registered subscribers via RPC
func Producer(c *cli.Context) {
	errc := make(chan error)
	conf := dagger.DefaultConfig(c)
	coordinator := dagger.NewConsulCoordinator(func(conf *consulapi.Config) {
		conf.Address = c.GlobalString("consul")
	})
	err := coordinator.Start(conf.RPCAdvertise)
	defer coordinator.Stop()
	if err != nil {
		log.Fatal("Error setting up coordinator")
	}

	lwmTracker := dagger.NewLWMTracker()
	// dispatcher := dagger.NewDispatcher(conf, coordinator)
	// bufferedDispatcher := dagger.StartBufferedDispatcher("test", dispatcher, lwmTracker, lwmTracker, make(chan struct{}))
	streamID := dagger.StreamID(c.String("streamID"))
	persister, err := dagger.NewPersister("/tmp/dagger")
	if err != nil {
		log.Fatalf("error opening database")
	}
	defer persister.Close()
	dispatcher := dagger.NewStreamDispatcher(streamID, coordinator, persister, lwmTracker, nil, nil)
	go dispatcher.Run(errc)

	reader := bufio.NewReader(os.Stdin)
	// var tmpT *dagger.Record
	for {
		var line string
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Println("error:", err)
			break
		}
		record, err := dagger.CreateRecord(streamID, strings.TrimSpace(line))
		if err != nil {
			log.Println("error:", err)
			break
		}
		log.Println("read", line)
		// bufferedDispatcher.ProcessRecord(record)
		persister.Insert(streamID, "p", record)
		dispatcher.ProcessRecord(record)
		// tmpT = record
	}
	// bufferedDispatcher.Stop()
	// tmpT.LWM = dagger.Timestamp(time.Now().UnixNano()).Add(time.Hour)
	// dispatcher.ProcessRecord(tmpT)
	time.Sleep(10000 * time.Second)
	log.Println("EXITING")
}
