package main

import (
	"os"

	"github.com/nsaje/dagger/command"

	"github.com/codegangsta/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "dagger"
	app.Usage = "user-centric real-time stream processing"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "consul",
			Usage: "Consul URL",
		},
	}
	app.Commands = []cli.Command{
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
			Name:    "producer",
			Aliases: []string{"p"},
			Usage:   "start a dedicated dagger producer node, which reads data from stdin and publishes it on the given stream",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "streamID, s",
					Usage: "Stream ID to publish tuples on",
				},
			},
			Action: command.Producer,
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
