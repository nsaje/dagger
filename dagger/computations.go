package dagger

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/natefinch/pie"
	"github.com/nsaje/dagger/structs"
	"github.com/rcrowley/go-metrics"
)

// ComputationManager manages computations that are being executed
type ComputationManager interface {
	SetupComputation(string) error
	Has(string) bool
}

// ComputationSyncer allows synchronizing computation status between workers
type ComputationSyncer interface {
	Sync(string) (*structs.ComputationSnapshot, error)
}

type computationManager struct {
	computations map[string]Computation
	coordinator  Coordinator
	receiver     *Receiver
	persister    Persister
	dispatcher   TupleProcessor

	counter metrics.Counter
}

// NewComputationManager returns an object that can manage computations
func NewComputationManager(coordinator Coordinator,
	receiver *Receiver,
	persister Persister,
	dispatcher TupleProcessor) *computationManager {
	c := metrics.NewCounter()
	metrics.Register("processing", c)
	cm := &computationManager{
		computations: make(map[string]Computation),
		coordinator:  coordinator,
		receiver:     receiver,
		persister:    persister,
		dispatcher:   dispatcher,
		counter:      c,
	}
	receiver.SetComputationSyncer(cm)
	return cm
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

	plugin, err := StartComputationPlugin(name, computationID)
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
		deduplicator, err := NewDeduplicator(computationID, cm.persister)
		if err != nil {
			return err
		}
		stopCh := make(chan struct{})

		groupHandler, err := cm.coordinator.JoinGroup(computationID)
		if err != nil {
			return err
		}

		lwmTracker := NewLWMTracker()
		// notify both LWM tracker and persister when a tuple is successfuly sent
		multiSentTracker := MultiSentTracker{[]SentTracker{lwmTracker, cm.persister}}

		bufferedDispatcher := StartBufferedDispatcher(computationID, cm.dispatcher, multiSentTracker, lwmTracker, stopCh)
		computation = &statefulComputation{
			computationID,
			lwmTracker,
			plugin,
			groupHandler,
			deduplicator,
			cm.persister,
			bufferedDispatcher,
			stopCh,
			sync.RWMutex{},
			false,
		}
	} else {
		computation = &statelessComputation{plugin, cm.dispatcher}
	}

	// run tuples through a linearizer so the computation plugin receives tuples
	// in order
	linearizer := NewLinearizer(computation, info.Inputs)
	for _, input := range info.Inputs {
		cm.receiver.SubscribeTo(input, linearizer)
	}
	cm.computations[computationID] = computation
	return nil
}

func (cm *computationManager) Has(computationID string) bool {
	_, has := cm.computations[computationID]
	return has
}

func (cm *computationManager) Sync(computationID string) (*structs.ComputationSnapshot, error) {
	comp, has := cm.computations[computationID]
	if !has {
		return nil, fmt.Errorf("Computation not found!")
	}
	return comp.Sync()
}

// Computation encapsulates all the stages of processing a tuple for a single
// computation
type Computation interface {
	TupleProcessor
	Sync() (*structs.ComputationSnapshot, error)
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
	return ProcessMultipleTuples(comp.dispatcher, response.Tuples)
}

func (comp *statelessComputation) Sync() (*structs.ComputationSnapshot, error) {
	return nil, nil
}

type statefulComputation struct {
	computationID      string
	lwmTracker         LwmTracker
	plugin             ComputationPlugin
	groupHandler       GroupHandler
	deduplicator       Deduplicator
	persister          Persister
	bufferedDispatcher TupleProcessor
	stopCh             chan struct{}

	sync.RWMutex // a reader/writer lock for blocking new tuples on sync request
	initialized  bool
}

func (comp *statefulComputation) syncStateWithMaster() error {
	comp.Lock()
	defer comp.Unlock()
	for !comp.initialized {
		log.Printf("[computations] Computation %s not initialized, syncing with group",
			comp.computationID)
		areWeLeader, currentLeader, err := comp.groupHandler.GetStatus()
		if err != nil {
			return err
		}
		if currentLeader == "" {
			// wait until a leader is chosen
			log.Println("[computations] Leader of ", comp.computationID, "not yet chosen, waiting")
			time.Sleep(time.Second)
			continue
		}
		if !areWeLeader {
			if err != nil {
				return err
			}
			leaderHandler, err := newMasterHandler(currentLeader)
			if err != nil {
				return fmt.Errorf("[computations] Error creating master handler: %s", err)
			}
			snapshot, err := leaderHandler.Sync(comp.computationID)
			if err != nil {
				return fmt.Errorf("[computations] Error syncing computation with master: %s", err)
			}
			err = comp.plugin.SetState(snapshot.PluginState)
			if err != nil {
				return fmt.Errorf("[computations] Error setting computation plugin state: %s", err)
			}
			err = comp.persister.ApplySnapshot(comp.computationID, snapshot)
			if err != nil {
				return fmt.Errorf("[computations] Error applying computation snapshot: %s", err)
			}
			// recreate deduplicator from newest received info
			deduplicator, err := NewDeduplicator(comp.computationID, comp.persister)
			if err != nil {
				return fmt.Errorf("[computations] Error recreating deduplicator after sync: %s", err)
			}
			comp.deduplicator = deduplicator
		}
		comp.initialized = true
	}
	return nil
}

func (comp *statefulComputation) ProcessTuple(t *structs.Tuple) error {
	comp.RLock()
	if !comp.initialized {
		comp.RUnlock()
		err := comp.syncStateWithMaster()
		if err != nil {
			return err
		}
	} else {
		comp.RUnlock()
	}

	// acquire a reader lock, so we wait in case there's synchronization with
	// a slave going on
	comp.RLock()
	defer comp.RUnlock()

	// deduplication
	seen, err := comp.deduplicator.Seen(t)
	if err != nil {
		return err
	}
	if seen {
		return nil
	}

	// calculate low water mark (LWM)
	// LWM = min(oldest event in this computation, LWM across all publishers this
	// computation is subscribed to)
	err = comp.lwmTracker.ProcessTuple(t)
	if err != nil {
		return err
	}

	// send it to the plugin for processing, but through the linearizer so
	// the plugin receives the tuples in order
	response, err := comp.plugin.SubmitTuple(t)
	if err != nil {
		return err
	}

	// calculate local low-water mark
	localLWM, err := comp.lwmTracker.GetLWM()
	if err != nil {
		return err
	}
	for _, t := range response.Tuples {
		t.LWM = localLWM
	}

	// persist info about received and produced tuples
	err = comp.persister.CommitComputation(comp.computationID, t, response.Tuples)
	if err != nil {
		return err
	}

	areWeLeader, _, err := comp.groupHandler.GetStatus()
	if err != nil {
		return err
	}

	if areWeLeader {
		comp.lwmTracker.BeforeDispatching(response.Tuples)
		// send to asynchronous dispatcher and return immediately
		return ProcessMultipleTuples(comp.bufferedDispatcher, response.Tuples)
	}
	// don't send downstream if we're not the leader of our group
	return nil
}

func (comp *statefulComputation) Sync() (*structs.ComputationSnapshot, error) {
	comp.Lock()
	defer comp.Unlock()
	snapshot, err := comp.persister.GetSnapshot(comp.computationID)
	if err != nil {
		return nil, err
	}
	pluginState, err := comp.plugin.GetState()
	if err != nil {
		return nil, err
	}
	snapshot.PluginState = pluginState
	return snapshot, nil
}

// StartComputationPlugin starts the plugin process
func StartComputationPlugin(name string, compID string) (ComputationPlugin, error) {
	log.Printf("[computations] Launching computation plugin '%s'", name)
	path := path.Join(os.Getenv("DAGGER_PLUGIN_PATH"), "computation-"+name)
	client, err := pie.StartProviderCodec(jsonrpc.NewClientCodec,
		os.Stderr,
		path,
	)
	if err != nil {
		return nil, fmt.Errorf("Error starting plugin %s: %s", name, err)
	}
	plugin := &computationPlugin{
		name:   name,
		compID: compID,
		client: client,
	}
	return plugin, nil
}

// ComputationPlugin handles the running and interacting with a computation
// plugin process
type ComputationPlugin interface {
	GetInfo(definition string) (*structs.ComputationPluginInfo, error)
	SubmitTuple(t *structs.Tuple) (*structs.ComputationPluginResponse, error)
	GetState() (*structs.ComputationPluginState, error)
	SetState(*structs.ComputationPluginState) error
}

type computationPlugin struct {
	client *rpc.Client
	name   string
	compID string
}

func (p *computationPlugin) GetInfo(definition string) (*structs.ComputationPluginInfo, error) {
	var result structs.ComputationPluginInfo
	err := p.client.Call("Computation.GetInfo", definition, &result)
	return &result, err
}

func (p *computationPlugin) GetState() (*structs.ComputationPluginState, error) {
	var result structs.ComputationPluginState
	err := p.client.Call("Computation.GetState", struct{}{}, &result)
	return &result, err
}

func (p *computationPlugin) SetState(state *structs.ComputationPluginState) error {
	var result string
	err := p.client.Call("Computation.SetState", state, &result)
	return err
}

func (p *computationPlugin) SubmitTuple(t *structs.Tuple) (*structs.ComputationPluginResponse, error) {
	var result structs.ComputationPluginResponse
	err := p.client.Call("Computation.SubmitTuple", t, &result)
	if err != nil {
		return nil, fmt.Errorf("Error submitting tuple to plugin %s: %s",
			p.name, err)
	}
	log.Println("[computations] got reply from plugin")
	for _, t := range result.Tuples {
		t.StreamID = p.compID
	}
	return &result, err
}

type masterHandler struct {
	client *rpc.Client
}

func newMasterHandler(addr string) (*masterHandler, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	client := jsonrpc.NewClient(conn)
	return &masterHandler{client}, nil
}

func (s *masterHandler) Sync(compID string) (*structs.ComputationSnapshot, error) {
	var reply structs.ComputationSnapshot
	err := s.client.Call("Receiver.Sync", compID, &reply)
	return &reply, err
}
