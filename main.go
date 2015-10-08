package main

import (
	"fmt"
	"os"

	"github.com/nsaje/dagger/command"

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
			Action:  command.Producer,
		},
		{
			Name:    "worker",
			Aliases: []string{"w"},
			Usage:   "start dagger node as a worker",
			Action:  command.Worker,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "appmetrics",
					Usage: "InfluxDB URL for app metrics (how many tuples are being processed etc.)",
				},
			},
		},
		{
			Name:    "subscriber",
			Aliases: []string{"s"},
			Usage:   "start dagger node as a topic subscriber",
			Action:  command.Subscriber,
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "dataonly",
					Usage: "print only the 'Data' field of the tuple if true",
				},
			},
		},
	}

	app.Run(os.Args)
}
