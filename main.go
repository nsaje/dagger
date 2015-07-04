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
		{
			Name:    "worker",
			Aliases: []string{"w"},
			Usage:   "start dagger node as a worker",
			Action:  worker,
		},
	}

	app.Run(os.Args)
}
