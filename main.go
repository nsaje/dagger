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

	app.Commands = []cli.Command{
		command.Worker,
		command.Producer,
		command.Subscriber,
	}

	app.Run(os.Args)
}
