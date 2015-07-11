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
	persister     Persister
}

// NewComputationManager returns an object that can manage computations
func NewComputationManager(persister Persister) ComputationManager {
	return &computationManager{
		compChannels:  make(map[string][]chan *structs.Tuple),
		subscriptions: make(map[string]string),
		output:        make(chan *structs.Tuple),
	}
}

func (cm *computationManager) SetupComputation(
	name string,
	inputs []string,
	output string) {

	cph := newComputationPluginHandler(name, cm.persister, cm.output)
	cph.Start()

	for _, input := range inputs {
		cm.compChannels[input] = append(cm.compChannels[input], cph.Input())
	}
}

func (cm *computationManager) ProcessComputations(in chan *structs.Tuple) chan *structs.Tuple {
	go func() {
		for t := range in {
			log.Printf("[computations] tuple: %v", t)
			compChans := cm.compChannels[t.StreamID]

			// Mark how many computations need to process this tuple
			t.Add(len(compChans))

			// Feed the tuple into interested computations
			for _, chann := range compChans {
				chann <- t
			}

			// ACK the tuple after all computations complete
			go func(t *structs.Tuple) {
				t.Wait()
				log.Println("[computations] ACKing tuple ", t)
				t.Ack()
			}(t)
		}
	}()
	return cm.output
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
	log.Println("[from-plugin] result: ", result)
	return result, err
}

// ComputationPluginHandler handles an instance of a computation plugin
type ComputationPluginHandler interface {
	Start()
	Input() chan *structs.Tuple
	Output() chan *structs.Tuple
}

type computationPluginHandler struct {
	pluginName string
	input      chan *structs.Tuple
	output     chan *structs.Tuple
	pending    []*structs.Tuple
	persister  Persister
}

func newComputationPluginHandler(name string, persister Persister, output chan *structs.Tuple) ComputationPluginHandler {
	return &computationPluginHandler{
		name,
		make(chan *structs.Tuple),
		output,
		make([]*structs.Tuple, 0),
		persister,
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
				cph.pending = append(cph.pending, t)
				if err != nil {
					log.Fatalf("error calling SubmitTuple(): %s", err) // FIXME error handling
				}
			case <-time.Tick(1000 * time.Millisecond):
				res, err := p.GetProductionsAndState()
				if err != nil {
					log.Fatalf("error calling GetProductionsAndState(): %s", err)
				}
				for i := range res.Tuples {
					cph.output <- &res.Tuples[i]
				}

				// persist state

				// mark that this computation is done with this tuple
				for _, t := range cph.pending {
					t.Done()
				}
				cph.pending = make([]*structs.Tuple, 0)
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
