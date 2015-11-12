package dagger

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"path"
	"sync"
	"time"

	"github.com/natefinch/pie"
	"github.com/nsaje/dagger/s"
)

// Computation encapsulates all the stages of processing a tuple for a single
// computation
type Computation interface {
	TupleProcessor
	Sync() (time.Time, error)
	GetSnapshot() (*s.ComputationSnapshot, error)
}

type statelessComputation struct {
	plugin     ComputationPlugin
	dispatcher TupleProcessor
}

func (comp *statelessComputation) ProcessTuple(t *s.Tuple) error {
	response, err := comp.plugin.SubmitTuple(t)
	if err != nil {
		return err
	}
	return ProcessMultipleTuples(comp.dispatcher, response.Tuples)
}

func (comp *statelessComputation) Sync() (time.Time, error) {
	return time.Time{}, nil
}

func (comp *statelessComputation) GetSnapshot() (*s.ComputationSnapshot, error) {
	return nil, nil
}

type statefulComputation struct {
	streamID     s.StreamID
	plugin       ComputationPlugin
	groupHandler GroupHandler
	linearizer   *Linearizer
	persister    Persister
	lwmTracker   LWMTracker
	dispatcher   TupleProcessor
	stopCh       chan struct{}

	sync.RWMutex // a reader/writer lock for blocking new tuples on sync request
	initialized  bool
}

func newStatefulComputation(streamID s.StreamID, coordinator Coordinator,
	persister Persister, plugin ComputationPlugin) (Computation, error) {
	groupHandler, err := coordinator.JoinGroup(streamID)
	if err != nil {
		return nil, err
	}
	stopCh := make(chan struct{})

	lwmTracker := NewLWMTracker()
	// notify both LWM tracker and persister when a tuple is successfuly sent
	// multiSentTracker := MultiSentTracker{[]SentTracker{lwmTracker, persister}}

	linearizer := NewLinearizer(streamID, persister, lwmTracker)
	// bufferedDispatcher := StartBufferedDispatcher(streamID, dispatcher, multiSentTracker, lwmTracker, stopCh)
	dispatcher := NewStreamDispatcher(streamID, coordinator, persister, lwmTracker, groupHandler)
	go dispatcher.Run()

	computation := &statefulComputation{
		streamID:     streamID,
		plugin:       plugin,
		groupHandler: groupHandler,
		persister:    persister,
		lwmTracker:   lwmTracker,
		linearizer:   linearizer,
		dispatcher:   dispatcher,
		stopCh:       stopCh,
		RWMutex:      sync.RWMutex{},
		initialized:  false,
	}

	linearizer.SetProcessor(computation)

	return computation, nil
}

func (comp *statefulComputation) Sync() (time.Time, error) {
	comp.Lock()
	defer comp.Unlock()
	var from time.Time
	for !comp.initialized {
		log.Printf("[computations] Computation %s not initialized, syncing with group",
			comp.streamID)
		areWeLeader, currentLeader, err := comp.groupHandler.GetStatus()
		if err != nil {
			return from, err
		}
		if currentLeader == "" {
			// wait until a leader is chosen
			log.Println("[computations] Leader of ", comp.streamID, "not yet chosen, waiting")
			time.Sleep(time.Second) // FIXME do this as a blocking call
			continue
		}
		if !areWeLeader {
			if err != nil {
				return from, err
			}
			leaderHandler, err := newMasterHandler(currentLeader)
			if err != nil {
				return from, fmt.Errorf("[computations] Error creating master handler: %s", err)
			}
			snapshot, err := leaderHandler.Sync(comp.streamID)
			if err != nil {
				return from, fmt.Errorf("[computations] Error syncing computation with master: %s", err)
			}
			err = comp.plugin.SetState(snapshot.PluginState)
			if err != nil {
				return from, fmt.Errorf("[computations] Error setting computation plugin state: %s", err)
			}
			err = comp.persister.ApplySnapshot(comp.streamID, snapshot)
			if err != nil {
				return from, fmt.Errorf("[computations] Error applying computation snapshot: %s", err)
			}
			// add one nanosecond so we don't take the last processed tuple again
			comp.linearizer.SetStartLWM(snapshot.LastTimestamp.Add(time.Nanosecond))
			from = snapshot.LastTimestamp.Add(time.Nanosecond)
			// // recreate deduplicator from newest received info
			// deduplicator, err := NewDeduplicator(comp.streamID, comp.persister)
			// if err != nil {
			// 	return fmt.Errorf("[computations] Error recreating deduplicator after sync: %s", err)
			// }
			// comp.deduplicator = deduplicator
		}
		comp.initialized = true
	}

	go comp.linearizer.StartForwarding()

	return from, nil
}

func (comp *statefulComputation) ProcessTuple(t *s.Tuple) error {
	err := comp.linearizer.ProcessTuple(t)
	if err != nil {
		return err
	}
	return nil
}

func (comp *statefulComputation) ProcessTupleLinearized(t *s.Tuple) error {
	// acquire a lock, so we wait in case there's synchronization with
	// a slave going on
	comp.Lock()
	defer comp.Unlock()

	// send it to the plugin for processing, but through the linearizer so
	// the plugin receives the tuples in order
	response, err := comp.plugin.SubmitTuple(t)
	if err != nil {
		// return err
	}

	// persist info about received and produced tuples
	err = comp.persister.CommitComputation(comp.streamID, t, response.Tuples)
	if err != nil {
		// return err
	}

	areWeLeader, _, err := comp.groupHandler.GetStatus()
	if err != nil {
		// return err
	}

	if areWeLeader {
		log.Println("WE ARE LEADER")
		comp.lwmTracker.BeforeDispatching(response.Tuples)
		// send to asynchronous dispatcher and return immediately
		ProcessMultipleTuples(comp.dispatcher, response.Tuples)
	} else {
		log.Println("WE ARE NOT LEADER", t)
	}
	// don't send downstream if we're not the leader of our group
	return nil
}

func (comp *statefulComputation) GetSnapshot() (*s.ComputationSnapshot, error) {
	log.Println("[computations] trying to acquire sync lock...")
	comp.Lock()
	log.Println("[computations] ... sync lock acquired!")
	defer comp.Unlock()
	snapshot, err := comp.persister.GetSnapshot(comp.streamID)
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
func StartComputationPlugin(name string, compID s.StreamID) (ComputationPlugin, error) {
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
	GetInfo(definition string) (*s.ComputationPluginInfo, error)
	SubmitTuple(t *s.Tuple) (*s.ComputationPluginResponse, error)
	GetState() (*s.ComputationPluginState, error)
	SetState(*s.ComputationPluginState) error
}

type computationPlugin struct {
	client *rpc.Client
	name   string
	compID s.StreamID
}

func (p *computationPlugin) GetInfo(definition string) (*s.ComputationPluginInfo, error) {
	var result s.ComputationPluginInfo
	err := p.client.Call("Computation.GetInfo", definition, &result)
	return &result, err
}

func (p *computationPlugin) GetState() (*s.ComputationPluginState, error) {
	var result s.ComputationPluginState
	err := p.client.Call("Computation.GetState", struct{}{}, &result)
	return &result, err
}

func (p *computationPlugin) SetState(state *s.ComputationPluginState) error {
	var result string
	err := p.client.Call("Computation.SetState", state, &result)
	return err
}

func (p *computationPlugin) SubmitTuple(t *s.Tuple) (*s.ComputationPluginResponse, error) {
	var result s.ComputationPluginResponse
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

func (mh *masterHandler) Sync(compID s.StreamID) (*s.ComputationSnapshot, error) {
	var reply s.ComputationSnapshot
	log.Println("[computations] issuing a sync request for computation", compID)
	err := mh.client.Call("Receiver.Sync", compID, &reply)
	return &reply, err
}
