package producers

import (
	"log"
	"net/rpc/jsonrpc"

	"github.com/natefinch/pie"
)

// Producer represents a specific producer implementation
type Producer struct {
	Stream chan string
	server pie.Server
}

// InitProducer initializes a new producer
func InitProducer() Producer {
	plugin := pie.NewProvider()
	producer := Producer{
		Stream: make(chan string),
		server: plugin,
	}
	if err := plugin.RegisterName("Producer", producer); err != nil {
		log.Fatalf("failed to register Plugin: %s", err)
	}
	go plugin.ServeCodec(jsonrpc.NewServerCodec)
	return producer
}

// GetNext returns the next value in the tuple stream
func (p Producer) GetNext(arg string, response *string) error {
	log.Printf("got call for GetNext")
	*response = <-p.Stream
	return nil
}
