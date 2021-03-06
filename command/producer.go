package command

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/codegangsta/cli"
	"github.com/nsaje/dagger/dagger"
)

// Producer reads records from stdin and submits them
// to registered subscribers via RPC
var Producer = cli.Command{
	Name:    "producer",
	Aliases: []string{"p"},
	Usage:   "start a dedicated dagger producer node, which reads data from stdin and publishes it on the given stream",
	Flags: mergeFlags(logFlags, consulFlags, persisterFlags, dispatcherFlags,
		[]cli.Flag{
			cli.StringFlag{
				Name:  "streamID, s",
				Usage: "Stream ID to publish records on",
			},
		}),
	Action: producerAction,
}

func producerAction(c *cli.Context) {
	initLogging(c)
	errc := make(chan error)
	coordinator := dagger.NewConsulCoordinator(consulConfFromFlags(c))
	err := coordinator.Start(nil, errc)
	defer coordinator.Stop()
	if err != nil {
		log.Fatal("Error setting up coordinator")
	}

	lwmTracker := dagger.NewLWMTracker()
	streamID := dagger.StreamID(c.String("streamID"))
	persister, err := dagger.NewPersister(persisterConfFromFlags(c))
	if err != nil {
		log.Fatalf("error opening database")
	}
	defer persister.Close()
	dispatcher := dagger.NewStreamDispatcher(streamID, coordinator, persister, lwmTracker, nil, dispatcherConfFromFlags(c))
	go dispatcher.Run(errc)

	coordinator.RegisterAsPublisher(streamID)
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
		fmt.Println("written")
	}
	handleSignals(errc)
}
