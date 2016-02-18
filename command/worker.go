package command

import (
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/nsaje/dagger/dagger"
	"github.com/rcrowley/go-metrics"
	"github.com/vrischmann/go-metrics-influxdb"

	"github.com/codegangsta/cli"
)

// Worker takes on computations. It registers as a subscriber for necessary
// topics and publishes the results of the computations
var Worker = cli.Command{
	Name:    "worker",
	Aliases: []string{"w"},
	Usage:   "start dagger node as a worker",
	Action:  workerAction,
	Flags: mergeFlags(logFlags, consulFlags, receiverFlags, persisterFlags, dispatcherFlags, httpApiFlags,
		[]cli.Flag{
			cli.StringFlag{
				Name:  "appmetrics",
				Usage: "InfluxDB URL for app metrics (how many records are being processed etc.)",
			},
		}),
}

func workerAction(c *cli.Context) {
	initLogging(c)
	appmetrics := c.String("appmetrics")
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
	errc := make(chan error)

	persister, err := dagger.NewPersister(persisterConfFromFlags(c))
	if err != nil {
		log.Fatalf("Error opening database")
	}
	defer persister.Close()

	coordinator := dagger.NewConsulCoordinator(consulConfFromFlags(c))
	receiver := dagger.NewReceiver(coordinator, receiverConfFromFlags(c))

	taskStarter := dagger.NewTaskStarter(coordinator, persister, dispatcherConfFromFlags(c))
	taskManager := dagger.NewTaskManager(coordinator, receiver, taskStarter)

	advertiseAddr := getAdvertiseAddr(c, receiver)
	err = coordinator.Start(advertiseAddr, errc)
	defer coordinator.Stop()
	if err != nil {
		log.Fatalf("Error starting coordinator %s", err)
	}

	go receiver.Listen(errc)
	go taskManager.ManageTasks()

	dispatcher := dagger.NewDispatcher(coordinator)
	httpapi := dagger.NewHttpAPI(coordinator, receiver, dispatcher)
	go httpapi.Serve()

	handleSignals(errc)
}
