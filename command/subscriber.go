package command

import (
	"fmt"
	"log"

	"bitbucket.org/nsaje/dagger/dagger"

	"github.com/codegangsta/cli"
)

// Subscriber registers as a subscriber for a certain topic(s). Useful for
// debugging.
func Subscriber(c *cli.Context) {
	conf := dagger.DefaultConfig()

	persister, err := dagger.NewPersister(conf)
	if err != nil {
		log.Fatalf("error opening database")
	}
	defer persister.Close()

	receiver := dagger.NewReceiver(conf)
	incoming := receiver.StartReceiving()

	coordinator := dagger.NewCoordinator(conf, receiver.ListenAddr())
	err = coordinator.Start()
	defer coordinator.Stop()
	if err != nil {
		log.Fatalf("Error starting coordinator %s", err)
	}
	log.Println("Coordinator started")

	topicGlob := c.Args().First()
	coordinator.SubscribeTo(topicGlob)
	log.Printf("Subscribed to %s", topicGlob)

	deduplicator := dagger.NewDeduplicator(persister)
	deduped := deduplicator.Deduplicate(incoming)

	go func() {
		for tuple := range deduped {
			fmt.Println("Received tuple:", tuple)
			tuple.Ack()
		}
	}()
	handleSignals()
}
