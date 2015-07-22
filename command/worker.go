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

	coordinator := dagger.NewCoordinator(conf, receiver.ListenAddr())
	err = coordinator.Start()
	defer coordinator.Stop()
	if err != nil {
		log.Fatalf("Error starting coordinator %s", err)
	}
	log.Println("Coordinator started")

	// set up pipeline
	dispatcher := dagger.NewDispatcher(conf, coordinator)
	compManager := dagger.NewComputationManager(coordinator, persister, dispatcher)

	receiver.StartReceiving(compManager)
	go coordinator.ManageJobs(compManager)

	// deduplicator := dagger.NewDeduplicator(persister)
	// deduped := deduplicator.Deduplicate(incoming)

	handleSignals()
}
