package main

import (
	"fmt"
	"os"

	"github.com/codegangsta/cli"
)

func die(format string, v ...interface{}) {
	fmt.Fprintln(os.Stderr, fmt.Sprintf(format, v...))
	os.Exit(1)
}

func dummy(ctx *cli.Context) {
	fmt.Println("hi")
	c, err := newCoordinator()
	if err != nil {
		die("Error starting coordinator %s", err)
	}
	err = c.RegisterTopic("mytopic")
	if err != nil {
		die("Error registering topic %s", err)
	}
	pubs, err := c.GetPublishers("mytopic")
	if err != nil {
		die("Error getting topic publishers %s", err)
	}
	fmt.Printf("mytopic: %s\n", pubs[0])
}

func main() {
	app := cli.NewApp()
	app.Name = "dagger"
	app.Usage = "user-centric real-time stream processing"
	app.Action = dummy
	app.Commands = []cli.Command{
		{
			Name:    "producer",
			Aliases: []string{"p"},
			Usage:   "start dagger node as a producer",
			Action:  producer,
		},
	}

	app.Run(os.Args)
}
