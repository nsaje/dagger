package command

import (
	"log"

	"bitbucket.org/nsaje/dagger/dagger"

	"github.com/codegangsta/cli"
)

// Worker takes on computations. It registers as a subscriber for necessary
// topics and publishes the results of the computations
func Worker(c *cli.Context) {
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
	coordinator.SubscribeTo("test")
	log.Println("Subscribed to test")

	deduplicator := dagger.NewDeduplicator(persister)
	deduped := deduplicator.Deduplicate(incoming)

	compManager := dagger.NewComputationManager(persister)
	compManager.SetupComputation("foo", []string{"test"}, "footest")
	compManager.SetupComputation("bar", []string{"test"}, "bartest")
	processed := compManager.ProcessComputations(deduped)

	dispatcher := dagger.NewDispatcher(conf, coordinator)
	go dispatcher.StartDispatching(processed)

	handleSignals()
}
