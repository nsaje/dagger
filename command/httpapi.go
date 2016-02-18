package command

import (
	"log"

	"github.com/nsaje/dagger/dagger"

	"github.com/codegangsta/cli"
)

// Worker takes on computations. It registers as a subscriber for necessary
// topics and publishes the results of the computations
var HTTPAPI = cli.Command{
	Name:    "httpapi",
	Aliases: []string{"h"},
	Usage:   "start dagger node as a HTTP API node",
	Action:  httpapiAction,
	Flags: mergeFlags(consulFlags, receiverFlags, persisterFlags, dispatcherFlags,
		[]cli.Flag{}),
}

func httpapiAction(c *cli.Context) {
	errc := make(chan error)

	persister, err := dagger.NewPersister(persisterConfFromFlags(c))
	if err != nil {
		log.Fatalf("Error opening database")
	}
	defer persister.Close()

	coordinator := dagger.NewConsulCoordinator(consulConfFromFlags(c))
	receiver := dagger.NewReceiver(coordinator, receiverConfFromFlags(c))

	advertiseAddr := getAdvertiseAddr(c, receiver)
	err = coordinator.Start(advertiseAddr, errc)
	defer coordinator.Stop()
	if err != nil {
		log.Fatalf("Error starting coordinator %s", err)
	}
	go receiver.Listen(errc)

	dispatcher := dagger.NewDispatcher(coordinator)
	httpapi := dagger.NewHttpAPI(coordinator, receiver, dispatcher)
	go httpapi.Serve()

	handleSignals(errc)
}
