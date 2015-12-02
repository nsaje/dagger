package command

import (
	"fmt"
	"log"
	"strconv"

	"github.com/nsaje/dagger/dagger"

	"github.com/codegangsta/cli"
)

// Subscriber registers as a subscriber for a certain topic(s). Useful for
// debugging.
var Subscriber = cli.Command{
	Name:    "subscriber",
	Aliases: []string{"s"},
	Usage:   "start dagger node as a topic subscriber",
	Action:  subscriberAction,
	Flags: mergeFlags(consulFlags, receiverFlags, persisterFlags,
		[]cli.Flag{
			cli.BoolFlag{
				Name:  "dataonly",
				Usage: "print only the 'Data' field of the record if true",
			},
			cli.StringFlag{
				Name:  "from",
				Value: "0",
				Usage: "subscribe to records from specified Unix nanosecond timestamp onward",
			},
		}),
}

func subscriberAction(c *cli.Context) {
	errc := make(chan error)
	persister, err := dagger.NewPersister(persisterConfFromFlags(c))
	if err != nil {
		log.Fatalf("error opening database")
	}
	defer persister.Close()

	prnter := &printer{dataonly: c.Bool("dataonly")}

	coordinator := dagger.NewConsulCoordinator(consulConfFromFlags(c))
	receiver := dagger.NewReceiver(coordinator, receiverConfFromFlags(c))
	go receiver.Listen(errc)

	advertiseAddr := getAdvertiseAddr(c, receiver)
	err = coordinator.Start(advertiseAddr, errc)
	defer coordinator.Stop()
	if err != nil {
		log.Fatalf("Error starting coordinator %s", err)
	}

	topicGlob := dagger.StreamID(c.Args().First())
	lwmTracker := dagger.NewLWMTracker()
	linearizer := dagger.NewLinearizer("test", persister, lwmTracker)
	linearizer.SetProcessor(prnter)
	go linearizer.Run(errc)
	from, err := strconv.ParseInt(c.String("from"), 10, 64)
	if err != nil {
		panic(err)
	}
	receiver.SubscribeTo(topicGlob, dagger.Timestamp(from), linearizer)
	log.Printf("Subscribed to %s", topicGlob)

	handleSignals(errc)
}

type printer struct {
	dataonly bool
}

func (p *printer) ProcessRecordLinearized(t *dagger.Record) error {
	log.Println("in printer")
	if p.dataonly {
		fmt.Println(t.Data)
	} else {
		fmt.Println(t)
	}
	return nil
}
