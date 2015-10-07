package command

import (
	"log"
	"time"

	"github.com/nsaje/dagger/dagger"
	"github.com/rcrowley/go-metrics"
	"github.com/vrischmann/go-metrics-influxdb"

	"github.com/codegangsta/cli"
)

// Worker takes on computations. It registers as a subscriber for necessary
// topics and publishes the results of the computations
func Worker(c *cli.Context) {
	// set up monitoring
	go influxdb.InfluxDB(
		metrics.DefaultRegistry, // metrics registry
		time.Second*1,           // interval
		"http://localhost:8086", // the InfluxDB url
		"dagger",                // your InfluxDB database
		"root",                  // your InfluxDB user
		"root",                  // your InfluxDB password
	)

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
