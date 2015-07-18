package dagger

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"strings"
	"time"

	"bitbucket.org/nsaje/dagger/structs"
	"github.com/natefinch/pie"
)

// ComputationManager manages computation that are being executed
type ComputationManager interface {
	SetupComputation(string)
	ProcessComputations(chan *structs.Tuple) chan *structs.Tuple
	TakeJobs()
}

type computationManager struct {
	compChannels  map[string][]chan *structs.Tuple
	subscriptions map[string]string
	output        chan *structs.Tuple
	coordinator   Coordinator
	persister     Persister
	jobs          map[string]struct{}
}

// NewComputationManager returns an object that can manage computations
func NewComputationManager(coordinator Coordinator, persister Persister) ComputationManager {
	return &computationManager{
		compChannels:  make(map[string][]chan *structs.Tuple),
		subscriptions: make(map[string]string),
		output:        make(chan *structs.Tuple),
		coordinator:   coordinator,
		persister:     persister,
		jobs:          make(map[string]struct{}),
	}
}

func (cm *computationManager) TakeJobs() {
	jobsWatch := cm.coordinator.WatchJobs()
	for newJobs := range jobsWatch {
		randomOrder := rand.Perm(len(newJobs))
		for _, i := range randomOrder {
			if _, alreadyTaken := cm.jobs[newJobs[i]]; alreadyTaken {
				continue
			}
			gotJob, err := cm.coordinator.TakeJob(newJobs[i])
			if err == nil {
				log.Println(err) // FIXME
			}
			if gotJob {
				log.Println("[computations] got job: ", newJobs[i])
				slashIdx := strings.LastIndex(newJobs[i], "/")
				cm.SetupComputation(newJobs[i][slashIdx+1:])
				cm.coordinator.RegisterAsPublisher(newJobs[i])
				cm.jobs[newJobs[i]] = struct{}{}
			}
		}
	}
}

func (cm *computationManager) SetupComputation(definition string) {
	computation, err := structs.ComputationFromString(definition)
	if err != nil {
		return // FIXME
	}

	// FIXME error handling
	cph := newComputationPluginHandler(computation.Name, cm.persister, cm.output)
	inputs, err := cph.Start(computation.Definition)

	log.Printf("INPUTS: ", inputs)
	for _, input := range inputs {
		cm.coordinator.SubscribeTo(input)
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

func (p computationPlugin) GetInputs(definition string) ([]string, error) {
	var result structs.InputsResponse
	log.Println("getting inputs for ", definition)
	err := p.client.Call("Computation.GetInputs", definition, &result)
	log.Printf("got inputs: %+v", result)
	return result.Inputs, err
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
	Start(string) ([]string, error)
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

func (cph *computationPluginHandler) Start(definition string) ([]string, error) {
	log.Printf("[computations] launching computation '%s'", cph.pluginName)
	client, err := pie.StartProviderCodec(jsonrpc.NewClientCodec, os.Stderr, "./computation-"+cph.pluginName)
	if err != nil {
		log.Fatalf("Error running plugin: %s", err)
	}
	p := computationPlugin{client}
	inputs, err := p.GetInputs(definition)
	if err != nil {
		return nil, err
	}
	go func() {
		defer client.Close()
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
					res.Tuples[i].StreamID = fmt.Sprintf("%s(%s)", cph.pluginName, definition)
					cph.output <- &res.Tuples[i]
				}

				// persist computation state
				state, err := json.Marshal(res.State)
				if err != nil { // FIXME error handling
					cph.persister.PersistState(cph.pluginName, state)
				}

				// store tuple IDs so we know we've processed them already
				log.Println("persisting: ", cph.pending, cph.persister)
				cph.persister.PersistReceivedTuples(cph.pending)

				// mark that this computation is done with this tuple
				// so senders can be ACKed
				for _, t := range cph.pending {
					t.Done()
				}
				cph.pending = make([]*structs.Tuple, 0)
			}
		}
	}()

	return inputs, nil
}

func (cph *computationPluginHandler) Input() chan *structs.Tuple {
	return cph.input
}

func (cph *computationPluginHandler) Output() chan *structs.Tuple {
	return cph.output
}
