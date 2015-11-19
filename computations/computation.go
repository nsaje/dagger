package computations

import (
	"log"
	"net/rpc/jsonrpc"
	"sync"

	"github.com/natefinch/pie"
	"github.com/nsaje/dagger/dagger"
)

// ComputationImplementation represents a specific computation implementation
type ComputationImplementation interface {
	GetInfo(definition string) (dagger.ComputationPluginInfo, error)
	SubmitRecord(t *dagger.Record) ([]*dagger.Record, error)
	GetState() ([]byte, error)
	SetState([]byte) error
}

// StartPlugin starts serving RPC requests and brokers data between RPC caller
// and the computation
func StartPlugin(impl ComputationImplementation) {
	provider := pie.NewProvider()
	computationPlugin := &ComputationPlugin{impl, sync.RWMutex{}}
	if err := provider.RegisterName("Computation", computationPlugin); err != nil {
		log.Fatalf("failed to register computation Plugin: %s", err)
	}
	provider.ServeCodec(jsonrpc.NewServerCodec)
}

// ComputationPlugin handles RPC calls from the main dagger process
type ComputationPlugin struct {
	impl ComputationImplementation
	mx   sync.RWMutex
}

// GetInfo returns the inputs to this computation
func (p *ComputationPlugin) GetInfo(definition string, response *dagger.ComputationPluginInfo) error {
	p.mx.RLock()
	defer p.mx.RUnlock()
	info, err := p.impl.GetInfo(definition)
	*response = info
	return err
}

// SubmitRecord submits the record into processing
func (p *ComputationPlugin) SubmitRecord(t *dagger.Record,
	response *dagger.ComputationPluginResponse) error {
	p.mx.Lock()
	defer p.mx.Unlock()
	*response = dagger.ComputationPluginResponse{}
	newRecords, err := p.impl.SubmitRecord(t)
	response.Records = newRecords
	return err
}

// GetState returns the dump of computation's state to dagger
func (p *ComputationPlugin) GetState(_ struct{},
	response *dagger.ComputationPluginState) error {
	p.mx.RLock()
	defer p.mx.RUnlock()
	*response = dagger.ComputationPluginState{}
	state, err := p.impl.GetState()
	response.State = state
	return err
}

// SetState seeds the state of the computation
func (p *ComputationPlugin) SetState(state *dagger.ComputationPluginState,
	response *string) error {
	p.mx.Lock()
	defer p.mx.Unlock()
	*response = "ok"
	err := p.impl.SetState(state.State)
	return err
}
