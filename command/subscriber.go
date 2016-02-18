package command

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"

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
	Flags: mergeFlags(logFlags, consulFlags, receiverFlags, persisterFlags,
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
			cli.StringFlag{
				Name:  "groupby",
				Usage: "match specified streams by specified tags (with @ sign)",
			},
		}),
}

func subscriberAction(c *cli.Context) {
	initLogging(c)
	fmt.Println(c.Args())
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

	if !c.IsSet("groupby") {
		receiver.SubscribeTo(topicGlob, dagger.Timestamp(from), linearizer)
		log.Printf("Subscribed to %s", topicGlob)
	} else {
		matchers := strings.Split(c.String("groupby"), ",")
		varargs := make([]interface{}, len(matchers))
		for i := range matchers {
			matchers[i] = strings.TrimSpace(matchers[i])
			varargs[i] = matchers[i]
		}
		testString := fmt.Sprintf(string(topicGlob), varargs...)
		if strings.Contains(testString, "%!") {
			log.Printf("Group by parameter has too few or too many values")
			return
		}
		// go match(topicGlob, matchers, errc)
	}

	handleSignals(errc)
}

// func match(topicGlob dagger.StreamID, matchers []string, errc chan error) {
// 	var matcherTags []string
// 	added := make(chan string)
// 	dropped := make(chan string)
// 	for _, matcher := range matchers {
// 		tags := dagger.ParseTags(dagger.StreamID(matcher))
// 		topic := string(dagger.StripTags(dagger.StreamID(matcher)))
// 		for k, v := range tags {
// 			if v == "@" {
// 				matcherTags = append(matcherTags, k)
// 				delete(tags, k)
// 			}
// 		}
// 		// go watchTag(topic, tags, matcherTags, added, dropped, errc)
// 	}
// }

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
