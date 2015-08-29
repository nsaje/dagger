package producers

import (
	"log"
	"net/rpc/jsonrpc"

	"github.com/nsaje/dagger/structs"

	"github.com/natefinch/pie"
)

// Producer represents a specific producer implementation
type Producer struct {
	Stream chan structs.Tuple
	server pie.Server
}

// InitProducer initializes a new producer
func InitProducer() Producer {
	plugin := pie.NewProvider()
	producer := Producer{
		Stream: make(chan structs.Tuple),
		server: plugin,
	}
	if err := plugin.RegisterName("Producer", producer); err != nil {
		log.Fatalf("failed to register Plugin: %s", err)
	}
	go plugin.ServeCodec(jsonrpc.NewServerCodec)
	return producer
}

// GetNext returns the next value in the tuple stream
func (p Producer) GetNext(arg string, response *structs.Tuple) error {
	log.Printf("got call for GetNext\n")
	*response = <-p.Stream
	log.Printf("response sent: %s", *response)
	return nil
}
