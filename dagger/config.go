package dagger

import (
	"time"

	"github.com/codegangsta/cli"
)

// Config is used to configure the node
type Config struct {
	SubscribersTTL time.Duration
	LevelDBFile    string
}

// DefaultConfig provides default config values
func DefaultConfig(c *cli.Context) *Config {
	conf := &Config{
		SubscribersTTL: time.Duration(15 * time.Second),
		LevelDBFile:    "dagger.db",
	}

	return conf
}
