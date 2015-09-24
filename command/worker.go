package command

import (
	"log"

	"github.com/nsaje/dagger/dagger"

	"github.com/codegangsta/cli"
)

// Worker takes on computations. It registers as a subscriber for necessary
// topics and publishes the results of the computations
func Worker(c *cli.Context) {
	conf := dagger.DefaultConfig()

	persister, err := dagger.NewPersister(conf)
	if err != nil {
		log.Fatalf("Error opening database")
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
	receiver.SetComputationSyncer(compManager)

	go receiver.ReceiveTuples(compManager)
	go coordinator.ManageJobs(compManager)

	// deduplicator := dagger.NewDeduplicator(persister)
	// deduped := deduplicator.Deduplicate(incoming)

	handleSignals()
}
