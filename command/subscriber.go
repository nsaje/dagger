package command

import (
	"fmt"
	"log"
	"strconv"

	"github.com/nsaje/dagger/dagger"

	"github.com/codegangsta/cli"
)

// Subscriber registers as a subscriber for a certain topic(s). Useful for
// debugging.
func Subscriber(c *cli.Context) {
	conf := dagger.DefaultConfig(c)

	persister, err := dagger.NewPersister("/tmp/dagger")
	if err != nil {
		log.Fatalf("error opening database")
	}
	defer persister.Close()

	prnter := &printer{dataonly: c.Bool("dataonly")}

	consulConf := dagger.DefaultConsulConfig()
	consulConf.Address = conf.ConsulAddr
	coordinator := dagger.NewCoordinator(consulConf)
	receiver := dagger.NewReceiver(conf, coordinator)
	go receiver.Listen()

	err = coordinator.Start(receiver.ListenAddr())
	defer coordinator.Stop()
	if err != nil {
		log.Fatalf("Error starting coordinator %s", err)
	}
	log.Println("Coordinator started")

	topicGlob := dagger.StreamID(c.Args().First())
	// linearizer := dagger.NewLinearizer(prnter, []string{topicGlob})
	// go linearizer.Linearize()
	lwmTracker := dagger.NewLWMTracker()
	linearizer := dagger.NewLinearizer("test", persister, lwmTracker)
	linearizer.SetProcessor(prnter)
	errc := make(chan error)
	go linearizer.Run(errc)
	from, err := strconv.ParseInt(c.String("from"), 10, 64)
	if err != nil {
		panic(err)
	}
	receiver.SubscribeTo(topicGlob, dagger.Timestamp(from), linearizer)
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

func (p *printer) ProcessRecordLinearized(t *dagger.Record) error {
	log.Println("in printer")
	if p.dataonly {
		fmt.Println(t.Data)
	} else {
		fmt.Println(t)
	}
	return nil
}
