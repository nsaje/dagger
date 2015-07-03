package main

import (
	"fmt"
	"log"

	"bitbucket.org/nsaje/dagger/structs"

	"github.com/codegangsta/cli"
)

func worker(c *cli.Context) {
	inc := make(chan structs.Tuple)
	conf := DefaultConfig()
	go startReceiving(conf, inc)

	coordinator, err := newCoordinator(conf)
	if err != nil {
		die("Error starting coordinator %s", err)
	}
	log.Println("Coordinator started")
	coordinator.SubscribeTo("test")
	log.Println("Subscribed to test")

	compManager := newComputationManager()
	compManager.SetupComputation("foo", []string{"test"}, "footest")
	compManager.SetupComputation("bar", []string{"test"}, "bartest")
	processed := compManager.processComputations(inc)
	for tuple := range processed {
		fmt.Println("Received tuple:", tuple)
	}
}
