package command

import (
	"bufio"
	"log"
	"os"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/nsaje/dagger/dagger"
)

// Producer reads records from stdin and submits them
// to registered subscribers via RPC
func Producer(c *cli.Context) {
	errc := make(chan error)
	coordinator := dagger.NewConsulCoordinator(func(conf *dagger.ConsulConfig) {
		conf.Address = c.GlobalString("consul")
	})
	err := coordinator.Start(nil)
	defer coordinator.Stop()
	if err != nil {
		log.Fatal("Error setting up coordinator")
	}

	lwmTracker := dagger.NewLWMTracker()
	streamID := dagger.StreamID(c.String("streamID"))
	persister, err := dagger.NewPersister("/tmp/dagger")
	if err != nil {
		log.Fatalf("error opening database")
	}
	defer persister.Close()
	dispatcher := dagger.NewStreamDispatcher(streamID, coordinator, persister, lwmTracker, nil, nil)
	go dispatcher.Run(errc)

	reader := bufio.NewReader(os.Stdin)
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
		persister.Insert(streamID, "p", record)
		dispatcher.ProcessRecord(record)
	}
	handleSignals()
}
