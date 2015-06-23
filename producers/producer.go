package producers

import (
	"bitbucket.org/nsaje/dagger/structs"
)

// Stream is a channel of tuples
type Stream chan structs.Tuple

// Producer produces tuples from the outside world
type Producer interface {
	StartProducing() <-chan Stream
}
