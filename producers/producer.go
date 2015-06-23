package producers

import "bitbucket.org/nsaje/dagger/producers/structs"

// ProducerPlugin creates tuples from the outside world
type ProducerPlugin struct {
	Name    string
	Streams map[string]chan Tuple
}

// Producer produces
type Producer interface {
	Init()
	Produce()
}
