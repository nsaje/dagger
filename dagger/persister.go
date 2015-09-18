package dagger

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/nsaje/dagger/structs"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/twinj/uuid"
)

const (
	receivedKeyFormat    = "%s-r-%s" // <computationID>-r-<tupleID>
	productionsKeyFormat = "%s-p-%s" // <computationID>-p-<tupleID>
)

// Persister takes care of persisting in-flight tuples and computation state
type Persister interface {
	Close()
	CommitComputation(compID string, in *structs.Tuple, out []*structs.Tuple) error
	GetSnapshot(compID string) (*structs.ComputationSnapshot, error)
	ApplySnapshot(compID string, snapshot *structs.ComputationSnapshot) error
	SentTracker
	ReceivedTracker
	StatePersister
}

// SentTracker deletes production entries that have been ACKed from the DB
type SentTracker interface {
	SentSuccessfuly(string, *structs.Tuple) error
}

// MultiSentTracker enables notifying multiple SentTrackers of a successfuly
// sent tuple
type MultiSentTracker struct {
	trackers []SentTracker
}

func (st MultiSentTracker) SentSuccessfuly(compID string, t *structs.Tuple) error {
	for _, tracker := range st.trackers {
		err := tracker.SentSuccessfuly(compID, t)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReceivedTracker persists info about which tuples we've already seen
type ReceivedTracker interface {
	PersistReceivedTuples(string, []*structs.Tuple) error
	GetRecentReceived(string) ([]string, error)
	ReceivedAlready(string, *structs.Tuple) (bool, error)
	// PruneOlderThan
}

// StatePersister persists the state of each computationManager
type StatePersister interface {
	PersistState(string, []byte)
}

// NewPersister initializes and returns a new Persister instance
func NewPersister(conf *Config) (Persister, error) {
	filename := "daggerDB-" + uuid.NewV4().String()
	db, err := leveldb.OpenFile(filename, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDBPersister{db, filename}, nil
}

// LevelDBPersister is built on top of LevelDB
type LevelDBPersister struct {
	db       *leveldb.DB
	filename string
}

// Close the persister
func (p *LevelDBPersister) Close() {
	p.db.Close()
}

// CommitComputation persists information about received and produced tuples
// atomically
func (p *LevelDBPersister) CommitComputation(compID string, in *structs.Tuple, out []*structs.Tuple) error {
	batch := new(leveldb.Batch)
	// mark incoming tuple as received
	log.Println("[persister]", p.filename, "committing", *in, "prods:", out)
	batch.Put([]byte(fmt.Sprintf(receivedKeyFormat, compID, in.ID)), nil)

	// save productions
	for _, t := range out {
		serialized, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("Error marshalling tuple %v: %s", t, err)
		}
		key := []byte(fmt.Sprintf(productionsKeyFormat, compID, t.ID))
		batch.Put(key, []byte(serialized))
	}
	return p.db.Write(batch, nil)
}

// GetSnapshot returns the snapshot of the computation's persisted state
func (p *LevelDBPersister) GetSnapshot(compID string) (*structs.ComputationSnapshot, error) {
	dbSnapshot, err := p.db.GetSnapshot()
	defer dbSnapshot.Release()
	if err != nil {
		return nil, fmt.Errorf("[persister]", err)
	}
	var snapshot structs.ComputationSnapshot
	// get received tuples
	snapshot.Received = make([]string, 0)
	keyPrefix := fmt.Sprintf(receivedKeyFormat, compID, "")
	iter := dbSnapshot.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()
	for iter.Next() {
		receivedID := iter.Key()[len(keyPrefix):]
		snapshot.Received = append(snapshot.Received, string(receivedID))
	}
	err = iter.Error()
	if err != nil {
		return nil, fmt.Errorf("[persister]", err)
	}

	// get produced tuples
	snapshot.Produced = make([]*structs.Tuple, 0)
	keyPrefix = fmt.Sprintf(productionsKeyFormat, compID, "")
	iter2 := dbSnapshot.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter2.Release()
	for iter2.Next() {
		if !strings.HasPrefix(string(iter.Key()), keyPrefix) {
			continue
		}
		var tuple structs.Tuple
		err := json.Unmarshal(iter.Value(), &tuple)
		if err != nil {
			return nil, fmt.Errorf("[persister]", err)
		}
		snapshot.Produced = append(snapshot.Produced, &tuple)
	}
	err = iter.Error()
	if err != nil {
		return nil, fmt.Errorf("[persister]", err)
	}
	return &snapshot, err
}

// ApplySnapshot applies the snapshot of the computation's persisted state
func (p *LevelDBPersister) ApplySnapshot(compID string, snapshot *structs.ComputationSnapshot) error {
	batch := new(leveldb.Batch)
	log.Println("[persister] applying snapshot", snapshot)

	// clear data for this computation
	keyPrefix := fmt.Sprintf("%s-", compID)
	iter := p.db.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()
	for iter.Next() {
		batch.Delete(iter.Key())
	}

	for _, r := range snapshot.Received {
		batch.Put([]byte(fmt.Sprintf(receivedKeyFormat, compID, r)), nil)
	}

	// save productions
	for _, t := range snapshot.Produced {
		serialized, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("Error marshalling tuple %v: %s", t, err)
		}
		key := []byte(fmt.Sprintf(productionsKeyFormat, compID, t.ID))
		batch.Put(key, []byte(serialized))
	}
	return p.db.Write(batch, nil)
}

// PersistState persists the state of the computation
func (p *LevelDBPersister) PersistState(compID string, state []byte) {
	return
}

// PersistReceivedTuples save the info about which tuples we've already seen
func (p *LevelDBPersister) PersistReceivedTuples(comp string, tuples []*structs.Tuple) error {
	batch := new(leveldb.Batch)
	for _, t := range tuples {
		batch.Put([]byte(fmt.Sprintf(receivedKeyFormat, comp, t.ID)), nil)
	}
	return p.db.Write(batch, nil)
}

// GetRecentReceived returns IDs of tuples we have recently received
func (p *LevelDBPersister) GetRecentReceived(comp string) ([]string, error) {
	keyPrefix := fmt.Sprintf(receivedKeyFormat, comp, "")
	iter := p.db.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	var recent []string
	for iter.Next() {
		key := string(iter.Key())
		recent = append(recent, key[len(keyPrefix):])
	}
	iter.Release()
	err := iter.Error()
	return recent, err
}

// ReceivedAlready returns whether we've seen this tuple before
func (p *LevelDBPersister) ReceivedAlready(comp string, t *structs.Tuple) (bool, error) {
	key := fmt.Sprintf(receivedKeyFormat, comp, t.ID)
	received, err := p.db.Has([]byte(key), nil)
	return received, err
}

// SentSuccessfuly deletes the production from the DB after it's been ACKed
func (p *LevelDBPersister) SentSuccessfuly(compID string, t *structs.Tuple) error {
	key := []byte(fmt.Sprintf(productionsKeyFormat, compID, t.ID))
	return p.db.Delete(key, nil)
}
