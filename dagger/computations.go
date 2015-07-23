package dagger

import (
	"fmt"
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"strings"

	"bitbucket.org/nsaje/dagger/structs"
	"github.com/natefinch/pie"
)

// ComputationManager manages computations that are being executed
type ComputationManager interface {
	SetupComputation(string) error
	ProcessTuple(*structs.Tuple) error
	Has(string) bool
}

type computationManager struct {
	computations  map[string]Computation
	subscriptions map[string][]TupleProcessor
	coordinator   Coordinator
	persister     Persister
	dispatcher    TupleProcessor
}

// NewComputationManager returns an object that can manage computations
func NewComputationManager(coordinator Coordinator,
	persister Persister,
	dispatcher TupleProcessor) ComputationManager {
	return &computationManager{
		computations:  make(map[string]Computation),
		subscriptions: make(map[string][]TupleProcessor),
		coordinator:   coordinator,
		persister:     persister,
		dispatcher:    dispatcher,
	}
}

// ParseComputationID parses a computation definition
func ParseComputationID(c string) (string, string, error) {
	c = strings.TrimSpace(c)
	firstParen := strings.Index(c, "(")
	if firstParen < 1 || c[len(c)-1] != ')' {
		return "", "", fmt.Errorf("Computation %s invalid!", c)
	}
	return c[:firstParen], c[firstParen+1 : len(c)-1], nil
}

func (cm *computationManager) SetupComputation(computationID string) error {
	name, definition, err := ParseComputationID(computationID)
	if err != nil {
		return err
	}

	plugin, err := StartComputationPlugin(name)
	if err != nil {
		return err
	}

	// get information about the plugin, such as which input streams it needs
	info, err := plugin.GetInfo(definition)
	if err != nil {
		return err
	}

	var computation Computation
	if info.Stateful {
		computation = &statefulComputation{plugin}
	} else {
		computation = &statelessComputation{plugin, cm.dispatcher}
	}

	for _, input := range info.Inputs {
		cm.coordinator.SubscribeTo(input)
		cm.subscriptions[input] = append(cm.subscriptions[input], computation)
	}
	cm.computations[computationID] = computation
	return nil
}

func (cm *computationManager) Has(computationID string) bool {
	_, has := cm.computations[computationID]
	return has
}

func (cm *computationManager) ProcessTuple(t *structs.Tuple) error {
	comps := cm.subscriptions[t.StreamID]
	log.Printf("[computations] tuple: %v, subscriptions:%v", t, comps)

	// Feed the tuple into interested computations
	return ProcessMultipleProcessors(comps, t)
}

// Computation encapsulates all the stages of processing a tuple for a single
// computation
type Computation interface {
	TupleProcessor
}

type statelessComputation struct {
	plugin     ComputationPlugin
	dispatcher TupleProcessor
}

func (comp *statelessComputation) ProcessTuple(t *structs.Tuple) error {
	response, err := comp.plugin.SubmitTuple(t)
	if err != nil {
		return err
	}
	// newTuples := make([]*structs.Tuple, len(response.Tuples))
	// for i, t := range response.Tuples {
	// 	newTuples[i] = &t
	// }
	return ProcessMultipleTuples(comp.dispatcher, response.Tuples)
}

type statefulComputation struct {
	plugin ComputationPlugin
}

func (comp *statefulComputation) ProcessTuple(*structs.Tuple) error {
	return nil
}

// StartComputationPlugin starts the plugin process
func StartComputationPlugin(name string) (ComputationPlugin, error) {
	log.Printf("[computations] launching computation plugin '%s'", name)
	client, err := pie.StartProviderCodec(jsonrpc.NewClientCodec,
		os.Stderr,
		"./computation-"+name)
	if err != nil {
		return nil, fmt.Errorf("Error starting plugin %s: %s", name, err)
	}
	plugin := &computationPlugin{
		name:   name,
		client: client,
	}
	return plugin, nil
}

// ComputationPlugin handles the running and interacting with a computation
// plugin process
type ComputationPlugin interface {
	GetInfo(definition string) (*structs.ComputationPluginInfo, error)
	SubmitTuple(t *structs.Tuple) (*structs.ComputationPluginResponse, error)
}

type computationPlugin struct {
	client *rpc.Client
	name   string
}

func (p *computationPlugin) GetInfo(definition string) (*structs.ComputationPluginInfo, error) {
	var result structs.ComputationPluginInfo
	err := p.client.Call("Computation.GetInfo", definition, &result)
	return &result, err
}

func (p *computationPlugin) SubmitTuple(t *structs.Tuple) (*structs.ComputationPluginResponse, error) {
	var result structs.ComputationPluginResponse
	err := p.client.Call("Computation.SubmitTuple", t, &result)
	if err != nil {
		return nil, fmt.Errorf("Error submitting tuple to plugin %s: %s",
			p.name, err)
	}
	return &result, err
}
