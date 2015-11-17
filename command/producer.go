package command

import (
	"bufio"
	"log"
	"os"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	"github.com/nsaje/dagger/consul"
	"github.com/nsaje/dagger/dagger"
	"github.com/nsaje/dagger/s"
)

// Producer reads records from stdin and submits them
// to registered subscribers via RPC
func Producer(c *cli.Context) {
	conf := dagger.DefaultConfig(c)
	consulConf := consul.DefaultConfig()
	consulConf.Address = conf.ConsulAddr
	coordinator := consul.NewCoordinator(consulConf)
	err := coordinator.Start(conf.RPCAdvertise)
	defer coordinator.Stop()
	if err != nil {
		log.Fatal("Error setting up coordinator")
	}

	lwmTracker := dagger.NewLWMTracker()
	// dispatcher := dagger.NewDispatcher(conf, coordinator)
	// bufferedDispatcher := dagger.StartBufferedDispatcher("test", dispatcher, lwmTracker, lwmTracker, make(chan struct{}))
	streamID := s.StreamID(c.String("streamID"))
	persister, err := dagger.NewPersister("/tmp/dagger")
	if err != nil {
		log.Fatalf("error opening database")
	}
	defer persister.Close()
	dispatcher := dagger.NewStreamDispatcher(streamID, coordinator, persister, lwmTracker, nil)
	go dispatcher.Run()

	reader := bufio.NewReader(os.Stdin)
	// var tmpT *s.Record
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
		persister.Insert1(streamID, "p", record)
		dispatcher.ProcessRecord(record)
		// tmpT = record
	}
	// bufferedDispatcher.Stop()
	// tmpT.LWM = s.Timestamp(time.Now().UnixNano()).Add(time.Hour)
	// dispatcher.ProcessRecord(tmpT)
	time.Sleep(10000 * time.Second)
	log.Println("EXITING")
}
