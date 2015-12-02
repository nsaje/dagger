package command

import (
	"log"
	"net"

	"github.com/codegangsta/cli"
	"github.com/nsaje/dagger/dagger"
)

func mergeFlags(flags ...[]cli.Flag) []cli.Flag {
	var all []cli.Flag
	for _, f := range flags {
		all = append(all, f...)
	}
	return all
}

var consulFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "consul",
		Usage: "Consul URL",
	},
	cli.StringFlag{
		Name:  "consul-ttl",
		Usage: "Consul TTL",
	},
	cli.DurationFlag{
		Name:  "consul-lockdelay",
		Usage: "Consul lock delay",
	},
}

func consulConfFromFlags(c *cli.Context) func(*dagger.ConsulConfig) {
	return func(conf *dagger.ConsulConfig) {
		if c.IsSet("consul") {
			conf.Address = c.String("consul")
		}
		if c.IsSet("consul-ttl") {
			conf.TTL = c.String("consul-ttl")
		}
		if c.IsSet("consul-lockdelay") {
			conf.LockDelay = c.Duration("consul-lockdelay")
		}
	}
}

func getAdvertiseAddr(c *cli.Context, receiver dagger.Receiver) net.Addr {
	advertiseAddr, ok := receiver.ListenAddr().(*net.TCPAddr)
	if !ok {
		log.Fatalf("not listening on TCP")
	}
	if c.IsSet("advertise") {
		advertiseAddr.IP = net.ParseIP(c.String("advertise"))
	}
	return advertiseAddr
}

var receiverFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "bind, b",
		Usage: "Which addres to bind RPC receiver to",
		Value: "0.0.0.0",
	},
	cli.StringFlag{
		Name:  "port, p",
		Usage: "Which port to bind RPC receiver to",
		Value: "46632",
	},
}

func receiverConfFromFlags(c *cli.Context) func(*dagger.ReceiverConfig) {
	return func(conf *dagger.ReceiverConfig) {
		if c.IsSet("bind") {
			conf.Addr = c.String("bind")
		}
		if c.IsSet("port") {
			conf.Port = c.String("port")
		}
	}
}

var persisterFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "data-dir",
		Usage: "Where to store Dagger state and record buffers",
	},
}

func persisterConfFromFlags(c *cli.Context) func(*dagger.PersisterConfig) {
	return func(conf *dagger.PersisterConfig) {
		if c.IsSet("data-dir") {
			conf.Dir = c.String("data-dir")
		}
	}
}

var dispatcherFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "pipelining-limit, pl",
		Usage: "How many records to send to subscribers without waiting for an ACK",
	},
}

func dispatcherConfFromFlags(c *cli.Context) func(*dagger.DispatcherConfig) {
	return func(conf *dagger.DispatcherConfig) {
		if c.IsSet("pipelining-limit") {
			conf.PipeliningLimit = c.Int("pipelining-limit")
		}
	}
}
