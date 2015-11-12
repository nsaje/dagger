package command

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nsaje/dagger/consul"
	"github.com/nsaje/dagger/dagger"
	"github.com/nsaje/dagger/structs"

	"github.com/codegangsta/cli"
)

// Subscriber registers as a subscriber for a certain topic(s). Useful for
// debugging.
func Subscriber(c *cli.Context) {
	conf := dagger.DefaultConfig(c)

	persister, err := dagger.NewPersister(conf)
	if err != nil {
		log.Fatalf("error opening database")
	}
	defer persister.Close()

	prnter := &printer{dataonly: c.Bool("dataonly")}

	consulConf := consul.DefaultConfig()
	consulConf.Address = conf.ConsulAddr
	coordinator := consul.NewCoordinator(consulConf)
	receiver := dagger.NewReceiver(conf, coordinator)
	go receiver.Listen()

	err = coordinator.Start(receiver.ListenAddr())
	defer coordinator.Stop()
	if err != nil {
		log.Fatalf("Error starting coordinator %s", err)
	}
	log.Println("Coordinator started")

	topicGlob := c.Args().First()
	// linearizer := dagger.NewLinearizer(prnter, []string{topicGlob})
	// go linearizer.Linearize()
	lwmTracker := dagger.NewLWMTracker()
	linearizer := dagger.NewLinearizer("test", persister, lwmTracker)
	linearizer.SetProcessor(prnter)
	go linearizer.StartForwarding()
	from, err := strconv.ParseInt(c.String("from"), 10, 64)
	if err != nil {
		panic(err)
	}
	receiver.SubscribeTo(topicGlob, time.Unix(0, from), linearizer)
	// receiver.SubscribeTo(topicGlob, linearizer)
	log.Printf("Subscribed to %s", topicGlob)

	// FIXME: bring deduplicator back into subscriber
	// deduplicator := dagger.NewDeduplicator(persister)
	// deduped := deduplicator.Deduplicate(incoming)
	handleSignals()
}

//
type printer struct {
	dataonly bool
}

func (p *printer) ProcessTupleLinearized(t *structs.Tuple) error {
	log.Println("in printer")
	if p.dataonly {
		fmt.Println(t.Data)
	} else {
		fmt.Println(t)
	}
	return nil
}
