package computations

import (
	"log"
	"net/rpc/jsonrpc"

	"bitbucket.org/nsaje/dagger/structs"

	"github.com/natefinch/pie"
)

// ComputationImplementation represents a specific computation implementation
type ComputationImplementation interface {
	GetInfo(definition string) (structs.ComputationPluginInfo, error)
	SubmitTuple(t *structs.Tuple) ([]*structs.Tuple, error)
}

// StartPlugin starts serving RPC requests and brokers data between RPC caller
// and the computation
func StartPlugin(impl ComputationImplementation) {
	provider := pie.NewProvider()
	computationPlugin := &ComputationPlugin{impl}
	if err := provider.RegisterName("Computation", computationPlugin); err != nil {
		log.Fatalf("failed to register computation Plugin: %s", err)
	}
	go provider.ServeCodec(jsonrpc.NewServerCodec)
}

// ComputationPlugin handles RPC calls from the main dagger process
type ComputationPlugin struct {
	impl ComputationImplementation
}

// GetInfo returns the inputs to this computation
func (p *ComputationPlugin) GetInfo(definition string, response *structs.ComputationPluginInfo) error {
	info, err := p.impl.GetInfo(definition)
	*response = info
	return err
}

// SubmitTuple submits the tuple into processing
func (p *ComputationPlugin) SubmitTuple(t *structs.Tuple,
	response *structs.ComputationPluginResponse) error {
	*response = structs.ComputationPluginResponse{}
	newTuples, err := p.impl.SubmitTuple(t)
	response.Tuples = newTuples
	return err
}
