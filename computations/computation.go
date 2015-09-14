package computations

import (
	"log"
	"net/rpc/jsonrpc"

	"github.com/nsaje/dagger/structs"

	"github.com/natefinch/pie"
)

// ComputationImplementation represents a specific computation implementation
type ComputationImplementation interface {
	GetInfo(definition string) (structs.ComputationPluginInfo, error)
	SubmitTuple(t *structs.Tuple) ([]*structs.Tuple, error)
	GetState() ([]byte, error)
	SetState([]byte) error
}

// StartPlugin starts serving RPC requests and brokers data between RPC caller
// and the computation
func StartPlugin(impl ComputationImplementation) {
	provider := pie.NewProvider()
	computationPlugin := &ComputationPlugin{impl}
	if err := provider.RegisterName("Computation", computationPlugin); err != nil {
		log.Fatalf("failed to register computation Plugin: %s", err)
	}
	provider.ServeCodec(jsonrpc.NewServerCodec)
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

// GetState returns the dump of computation's state to dagger
func (p *ComputationPlugin) GetState(_ struct{},
	response *structs.ComputationPluginState) error {
	*response = structs.ComputationPluginState{}
	state, err := p.impl.GetState()
	response.State = state
	return err
}

// SetState seeds the state of the computation
func (p *ComputationPlugin) SetState(state *structs.ComputationPluginState,
	response *string) error {
	*response = "ok"
	err := p.impl.SetState(state.State)
	return err
}
