package command

import (
	"net"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/codegangsta/cli"
	"github.com/nsaje/dagger/dagger"
)

func initLogging(c *cli.Context) {
	if !c.IsSet("log") {
		return
	}
	log.SetOutput(&lumberjack.Logger{
		Filename:   c.String("log"),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28, //days
	})
}

var logFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "log",
		Usage: "logging path, log to stdout if empty",
	},
}

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
		Name:  "iface, i",
		Usage: "Which interface to bind RPC receiver to",
		Value: "",
	},
	cli.StringFlag{
		Name:  "port, p",
		Usage: "Which port to bind RPC receiver to",
		Value: "46632",
	},
}

func getIfaceAddr(ifaceName string) string {
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return ""
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				log.Warnln("Returning IP ", ipnet.IP.String())
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func receiverConfFromFlags(c *cli.Context) func(*dagger.ReceiverConfig) {
	return func(conf *dagger.ReceiverConfig) {
		if c.IsSet("iface") {
			addr := getIfaceAddr(c.String("iface"))
			if len(addr) == 0 {
				log.Warnf("Interface %s invalid", c.String("iface"))
			} else {
				conf.Addr = addr
			}
		}
		if c.IsSet("port") {
			conf.Port = c.String("port")
		}
	}
}

var httpApiFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "api-port, ap",
		Usage: "Which port to bind HTTP API to",
		Value: "46666",
	},
}

func httpapiConfFromFlags(c *cli.Context) func(*dagger.HttpAPIConfig) {
	return func(conf *dagger.HttpAPIConfig) {
		if c.IsSet("api-port") {
			conf.Port = c.String("api-port")
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
