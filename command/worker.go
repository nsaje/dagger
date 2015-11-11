package command

import (
	"fmt"
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

	appmetrics := c.String("appmetrics")
	log.Println("Appmetrics: ", appmetrics)
	if len(appmetrics) > 0 {
		// set up monitoring
		go influxdb.InfluxDB(
			metrics.DefaultRegistry, // metrics registry
			time.Second*1,           // interval
			appmetrics,              // the InfluxDB url
			"dagger",                // your InfluxDB database
			"root",                  // your InfluxDB user
			"root",                  // your InfluxDB password
		)
	}

	conf := dagger.DefaultConfig(c)
	fmt.Println("ARGS:", c.GlobalFlagNames())

	persister, err := dagger.NewPersister(conf)
	if err != nil {
		log.Fatalf("Error opening database")
	}
	defer persister.Close()

	coordinator := dagger.NewCoordinator(conf)
	receiver := dagger.NewReceiver(conf, coordinator)
	dispatcher := dagger.NewDispatcher(conf, coordinator)
	compManager := dagger.NewComputationManager(
		coordinator, receiver, persister, dispatcher)
	receiver.SetComputationSyncer(compManager)
	// httpAPI := dagger.NewHttpAPI(receiver, dispatcher)

	err = coordinator.Start(receiver.ListenAddr())
	defer coordinator.Stop()
	if err != nil {
		log.Fatalf("Error starting coordinator %s", err)
	}
	log.Println("Coordinator started")

	go receiver.ReceiveTuples()
	go coordinator.ManageJobs(compManager)

	// go httpAPI.Serve()

	// deduplicator := dagger.NewDeduplicator(persister)
	// deduped := deduplicator.Deduplicate(incoming)

	handleSignals()
}
