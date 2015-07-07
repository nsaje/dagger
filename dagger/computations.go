package dagger

import (
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"time"

	"bitbucket.org/nsaje/dagger/structs"
	"github.com/natefinch/pie"
)

// ComputationManager manages computation that are being executed
type ComputationManager interface {
	SetupComputation(name string, inputs []string, output string)
	ProcessComputations(chan *structs.Tuple) chan *structs.Tuple
}

type computationManager struct {
	compChannels  map[string][]chan *structs.Tuple
	subscriptions map[string]string
	output        chan *structs.Tuple
}

// NewComputationManager returns an object that can manage computations
func NewComputationManager() ComputationManager {
	return &computationManager{
		compChannels:  make(map[string][]chan *structs.Tuple),
		subscriptions: make(map[string]string),
		output:        make(chan *structs.Tuple),
	}
}

// ComputationPluginHandler handles an instance of a computation plugin
type ComputationPluginHandler interface {
	Start()
	Input() chan *structs.Tuple
	Output() chan *structs.Tuple
}

type computationPlugin struct {
	client *rpc.Client
}

func (p computationPlugin) SubmitTuple(t *structs.Tuple) (result string, err error) {
	err = p.client.Call("Computation.SubmitTuple", t, &result)
	return result, err
}

func (p computationPlugin) GetProductionsAndState() (result structs.ComputationResponse, err error) {
	err = p.client.Call("Computation.GetProductionsAndState", "", &result)
	return result, err
}

type computationPluginHandler struct {
	pluginName string
	input      chan *structs.Tuple
	output     chan *structs.Tuple
}

func newComputationPluginHandler(name string, output chan *structs.Tuple) ComputationPluginHandler {
	return &computationPluginHandler{
		name,
		make(chan *structs.Tuple),
		output,
	}
}

func (cph *computationPluginHandler) Start() {
	go func(pluginPath string) {
		log.Printf("launching computation '%s'", cph.pluginName)
		client, err := pie.StartProviderCodec(jsonrpc.NewClientCodec, os.Stderr, pluginPath)
		if err != nil {
			log.Fatalf("Error running plugin: %s", err)
		}
		defer client.Close()
		p := computationPlugin{client}
		for {
			select {
			case t := <-cph.input:
				_, err := p.SubmitTuple(t)
				if err != nil {
					log.Fatalf("error calling SubmitTuple(): %s", err)
				}
			case <-time.Tick(100 * time.Millisecond):
				res, err := p.GetProductionsAndState()
				if err != nil {
					log.Fatalf("error calling GetProductionsAndState(): %s", err)
				}
				for _, t := range res.Tuples {
					cph.output <- &t
				}
			}
		}
	}("./computation-" + cph.pluginName)
}

func (cph *computationPluginHandler) Input() chan *structs.Tuple {
	return cph.input
}

func (cph *computationPluginHandler) Output() chan *structs.Tuple {
	return cph.output
}

func (cm *computationManager) SetupComputation(
	name string,
	inputs []string,
	output string) {

	cph := newComputationPluginHandler(name, cm.output)
	cph.Start()

	for _, input := range inputs {
		cm.compChannels[input] = append(cm.compChannels[input], cph.Input())
	}
}

func (cm *computationManager) ProcessComputations(in chan *structs.Tuple) chan *structs.Tuple {
	go func() {
		for t := range in {
			log.Printf("processComputations: processing tuple: %v", t)
			// if t.Data.(float64) == 0 {
			// 	time.AfterFunc(5*time.Second, func() {
			// 		panic("here:")
			// 	})
			// }
			compChans := cm.compChannels[t.StreamID]
			for _, chann := range compChans {
				chann <- t
			}
		}
	}()
	return cm.output
}
