package command

import (
	"bufio"
	"log"
	"os"
	"strings"

	"github.com/nsaje/dagger/dagger"

	"github.com/codegangsta/cli"
)

// Producer reads tuples from stdin and submits them
// to registered subscribers via RPC
func Producer(c *cli.Context) {
	conf := dagger.DefaultConfig(c)
	coordinator := dagger.NewCoordinator(conf)
	coordinator.SetAddr(conf.RPCAdvertise)
	err := coordinator.Start()
	defer coordinator.Stop()
	if err != nil {
		log.Fatal("Error setting up coordinator")
	}

	dispatcher := dagger.NewDispatcher(conf, coordinator)
	streamID := c.String("streamID")
	reader := bufio.NewReader(os.Stdin)
	for {
		var line string
		line, err = reader.ReadString('\n')
		if err != nil {
			log.Println("error:", err)
			return
		}
		tuple, _ := dagger.CreateTuple(streamID, strings.TrimSpace(line))
		dispatcher.ProcessTuple(tuple)
	}
}
