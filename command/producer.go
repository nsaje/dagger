package command

import (
	"bufio"
	"log"
	"os"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	"github.com/nsaje/dagger/dagger"
	"github.com/nsaje/dagger/structs"
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

	lwmTracker := dagger.NewLWMTracker()
	dispatcher := dagger.NewDispatcher(conf, coordinator)
	bufferedDispatcher := dagger.StartBufferedDispatcher("test", dispatcher, lwmTracker, lwmTracker, make(chan struct{}))
	streamID := c.String("streamID")

	reader := bufio.NewReader(os.Stdin)
	var tmpT *structs.Tuple
	for {
		var line string
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Println("error:", err)
			break
		}
		tuple, err := dagger.CreateTuple(streamID, strings.TrimSpace(line))
		if err != nil {
			log.Println("error:", err)
			break
		}
		log.Println("read", line)
		lwmTracker.BeforeDispatching([]*structs.Tuple{tuple})
		tuple.LWM, err = lwmTracker.GetLocalLWM()
		if err != nil {
			log.Println("error:", err)
			break
		}
		bufferedDispatcher.ProcessTuple(tuple)
		tmpT = tuple
	}
	bufferedDispatcher.Stop()
	tmpT.LWM = time.Now().Add(time.Hour)
	dispatcher.ProcessTuple(tmpT)
	log.Println("EXITING")
}
