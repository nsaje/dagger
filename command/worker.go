package command

import (
	"log"
	"net"
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

	persister, err := dagger.NewPersister("/tmp/dagger")
	if err != nil {
		log.Fatalf("Error opening database")
	}
	defer persister.Close()

	coordinator := dagger.NewConsulCoordinator(func(conf *dagger.ConsulConfig) {
		conf.Address = c.GlobalString("consul")
	})

	receiver := dagger.NewReceiver(coordinator, func(conf *dagger.ReceiverConfig) {
		if c.IsSet("bind") {
			conf.Addr = c.String("bind")
		}
		if c.IsSet("port") {
			conf.Port = c.String("port")
		}
	})

	taskStarter := dagger.NewTaskStarter(coordinator, persister)
	taskManager := dagger.NewTaskManager(coordinator, receiver, taskStarter)

	advertiseAddr, ok := receiver.ListenAddr().(*net.TCPAddr)
	if !ok {
		log.Fatalf("not listening on TCP")
	}
	if c.IsSet("advertise") {
		advertiseAddr.IP = net.ParseIP(c.String("advertise"))
	}
	err = coordinator.Start(advertiseAddr)
	defer coordinator.Stop()
	if err != nil {
		log.Fatalf("Error starting coordinator %s", err)
	}

	go receiver.Listen()
	go taskManager.ManageTasks()

	handleSignals()
}
