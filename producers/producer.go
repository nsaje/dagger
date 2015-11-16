package producers

import (
	"log"
	"net/rpc/jsonrpc"

	"github.com/nsaje/dagger/s"

	"github.com/natefinch/pie"
)

// Producer represents a specific producer implementation
type Producer struct {
	Stream chan s.Record
	server pie.Server
}

// InitProducer initializes a new producer
func InitProducer() Producer {
	plugin := pie.NewProvider()
	producer := Producer{
		Stream: make(chan s.Record),
		server: plugin,
	}
	if err := plugin.RegisterName("Producer", producer); err != nil {
		log.Fatalf("failed to register Plugin: %s", err)
	}
	go plugin.ServeCodec(jsonrpc.NewServerCodec)
	return producer
}

// GetNext returns the next value in the record stream
func (p Producer) GetNext(arg string, response *s.Record) error {
	log.Printf("got call for GetNext\n")
	*response = <-p.Stream
	log.Printf("response sent: %s", *response)
	return nil
}
