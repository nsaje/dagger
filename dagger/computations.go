package dagger

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"path"
	"sync"
	"time"

	"github.com/natefinch/pie"
)

type statefulComputation struct {
	streamID     StreamID
	plugin       ComputationPlugin
	groupHandler GroupHandler
	linearizer   *Linearizer
	persister    TaskPersister
	lwmTracker   LWMTracker
	dispatcher   *StreamDispatcher
	stopCh       chan struct{}

	sync.RWMutex // a reader/writer lock for blocking new records on sync request
	initialized  bool
}

func newStatefulComputation(streamID StreamID, coordinator Coordinator,
	persister Persister, plugin ComputationPlugin, dispatcherConfig func(*DispatcherConfig)) (*statefulComputation, error) {
	groupHandler, err := coordinator.JoinGroup(streamID)
	if err != nil {
		return nil, err
	}
	stopCh := make(chan struct{})

	lwmTracker := NewLWMTracker()
	// notify both LWM tracker and persister when a record is successfuly sent
	// multiSentTracker := MultiSentTracker{[]SentTracker{lwmTracker, persister}}

	linearizer := NewLinearizer(streamID, persister, lwmTracker)
	// bufferedDispatcher := StartBufferedDispatcher(streamID, dispatcher, multiSentTracker, lwmTracker, stopCh)
	dispatcher := NewStreamDispatcher(streamID, coordinator, persister, lwmTracker, groupHandler, dispatcherConfig)

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

func (comp *statefulComputation) GetSnapshot() ([]byte, error) {
	log.Println("[computations] trying to acquire sync lock...")
	comp.Lock()
	log.Println("[computations] ... sync lock acquired!")
	defer comp.Unlock()
	snapshot := make(map[string][]byte)
	persisterSnapshot, err := comp.persister.GetSnapshot(comp.streamID)
	if err != nil {
		return nil, errors.New("stateful computation: " + err.Error())
	}
	pluginSnapshot, err := comp.plugin.GetSnapshot()
	if err != nil {
		return nil, errors.New("stateful computation: " + err.Error())
	}
	snapshot["persister"] = persisterSnapshot
	snapshot["plugin"] = pluginSnapshot
	return json.Marshal(snapshot)
}

func (comp *statefulComputation) Sync() (Timestamp, error) {
	comp.Lock()
	defer comp.Unlock()
	var from Timestamp
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

			snapshotMap := make(map[string][]byte)
			err = json.Unmarshal(snapshot, &snapshotMap)
			if err != nil {
				return from, fmt.Errorf("cannot unmarshal snapshot: %s:", err.Error())
			}

			err = comp.plugin.ApplySnapshot(snapshotMap["plugin"])
			if err != nil {
				return from, fmt.Errorf("[computations] Error setting computation plugin state: %s", err)
			}
			err = comp.persister.ApplySnapshot(comp.streamID, snapshotMap["persister"])
			if err != nil {
				return from, fmt.Errorf("[computations] Error applying computation snapshot: %s", err)
			}

			from, err = comp.persister.GetLastTimestamp(comp.streamID)
			if err != nil {
				from = Timestamp(0)
				// return from, fmt.Errorf("[computations] error getting last timestamp")
			}
			// add one nanosecond so we don't take the last processed record again
			from++
			comp.linearizer.SetStartLWM(from)
			// // recreate deduplicator from newest received info
			// deduplicator, err := NewDeduplicator(comp.streamID, comp.persister)
			// if err != nil {
			// 	return fmt.Errorf("[computations] Error recreating deduplicator after sync: %s", err)
			// }
			// comp.deduplicator = deduplicator
		}
		comp.initialized = true
	}

	return from, nil
}

func (comp *statefulComputation) Run(errc chan error) {
	go comp.dispatcher.Run(errc)
	go comp.linearizer.Run(errc)
}

func (comp *statefulComputation) Stop() {
	comp.linearizer.Stop()
	comp.plugin.Stop()
	comp.dispatcher.Stop()
}

func (comp *statefulComputation) ProcessRecord(r *Record) error {
	log.Println("[linearizer] processing", r)
	err := comp.linearizer.ProcessRecord(r)
	if err != nil {
		return err
	}
	return nil
}

func (comp *statefulComputation) ProcessRecordLinearized(t *Record) error {
	// acquire a lock, so we wait in case there's synchronization with
	// a slave going on
	comp.Lock()
	defer comp.Unlock()

	// send it to the plugin for processing, but through the linearizer so
	// the plugin receives the records in order
	response, err := comp.plugin.SubmitRecord(t)
	if err != nil {
		log.Println("ERROR:", err)
		return err
	}

	// persist info about received and produced records
	err = comp.persister.CommitComputation(comp.streamID, t, response.Records)
	if err != nil {
		return err
	}

	areWeLeader, _, err := comp.groupHandler.GetStatus()
	if err != nil {
		return err
	}

	if areWeLeader {
		log.Println("WE ARE LEADER")
		comp.lwmTracker.BeforeDispatching(response.Records)
		// send to asynchronous dispatcher and return immediately
		ProcessMultipleRecords(comp.dispatcher, response.Records)
	} else {
		log.Println("WE ARE NOT LEADER", t)
	}
	// don't send downstream if we're not the leader of our group
	return nil
}

// StartComputationPlugin starts the plugin process
func StartComputationPlugin(name string, compID StreamID) (ComputationPlugin, error) {
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
	GetInfo(definition string) (*ComputationPluginInfo, error)
	SubmitRecord(t *Record) (*ComputationPluginResponse, error)
	GetSnapshot() ([]byte, error)
	ApplySnapshot([]byte) error
	Stop() error
}

type computationPlugin struct {
	client *rpc.Client
	name   string
	compID StreamID
}

func (p *computationPlugin) GetInfo(definition string) (*ComputationPluginInfo, error) {
	var result ComputationPluginInfo
	err := p.client.Call("Computation.GetInfo", definition, &result)
	return &result, err
}

func (p *computationPlugin) GetSnapshot() ([]byte, error) {
	var result []byte
	err := p.client.Call("Computation.GetState", struct{}{}, &result)
	if err != nil {
		return nil, errors.New("plugin snapshot: " + err.Error())
	}
	return result, err
}

func (p *computationPlugin) ApplySnapshot(state []byte) error {
	var result string
	err := p.client.Call("Computation.SetState", state, &result)
	return err
}

func (p *computationPlugin) SubmitRecord(r *Record) (*ComputationPluginResponse, error) {
	log.Println("[plugin] processing", r)
	var result ComputationPluginResponse
	err := p.client.Call("Computation.SubmitRecord", r, &result)
	if err != nil {
		return nil, fmt.Errorf("Error submitting record to plugin %s: %s",
			p.name, err)
	}
	for _, r := range result.Records {
		r.StreamID = p.compID
	}
	return &result, err
}

func (p *computationPlugin) Stop() error {
	log.Println("[plugin] stopping", p.name)
	return p.client.Close()
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

func (mh *masterHandler) Sync(compID StreamID) ([]byte, error) {
	var reply []byte
	log.Println("[computations] issuing a sync request for computation", compID)
	err := mh.client.Call("RPCHandler.Sync", compID, &reply)
	return reply, err
}
