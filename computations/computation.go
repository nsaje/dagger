package computations

import (
	"log"
	"net/rpc/jsonrpc"

	"bitbucket.org/nsaje/dagger/structs"

	"github.com/natefinch/pie"
)

// Computation represents a specific computation implementation
type Computation interface {
	Input() chan structs.Tuple
	Output() chan structs.Tuple
	State() interface{}
}

// DefaultComputation represents the default computation implementation
type DefaultComputation struct {
	input  chan structs.Tuple
	output chan structs.Tuple
}

// Input is a channel incoming tuples are published on
func (c *DefaultComputation) Input() chan structs.Tuple {
	return c.input
}

// Output is a channel resulting tuples are published on
func (c *DefaultComputation) Output() chan structs.Tuple {
	return c.output
}

// NewDefaultComputation returns a new default computation
func NewDefaultComputation() *DefaultComputation {
	return &DefaultComputation{
		input:  make(chan structs.Tuple, 100),
		output: make(chan structs.Tuple, 100),
	}
}

// ComputationPlugin is a plugin performing computations on plugins
type ComputationPlugin struct {
	computation Computation
}

// StartPlugin starts serving RPC requests and brokers data between RPC caller
// and the computation
func StartPlugin(comp Computation) {
	provider := pie.NewProvider()
	computationPlugin := &ComputationPlugin{
		computation: comp,
	}
	if err := provider.RegisterName("Computation", computationPlugin); err != nil {
		log.Fatalf("failed to register computation Plugin: %s", err)
	}
	go provider.ServeCodec(jsonrpc.NewServerCodec)
}

// SubmitTuple submits the tuple into processing
func (p *ComputationPlugin) SubmitTuple(arg *structs.Tuple,
	response *string) error {
	log.Printf("got call for Process\n")
	p.computation.Input() <- *arg
	*response = "ok"
	log.Printf("response sent: %s", *response)
	return nil
}

// GetProductionsAndState gets new productions and current
// state of the computation
func (p *ComputationPlugin) GetProductionsAndState(arg string,
	response *structs.ComputationResponse) error {
	log.Printf("got call for GetProductionsAndState\n")
	// drain output buffer, TODO: do this more efficiently
LOOP:
	for {
		select {
		case t := <-p.computation.Output():
			response.Tuples = append(response.Tuples, t)
		default:
			break LOOP
		}
	}
	response.State = p.computation.State()
	log.Printf("response sent: %s", *response)
	return nil
}
