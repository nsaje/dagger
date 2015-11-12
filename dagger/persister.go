package dagger

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nsaje/dagger/s"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/twinj/uuid"
)

const (
	receivedKeyFormat    = "%s-r-%s"    // <streamID>-r-<tupleID>
	productionsKeyFormat = "%s-p-%d-%s" // <streamID>-p-<timestamp>-<tupleID>
	inKeyFormat          = "%s-i-%d-%s" // <streamID>-i-<timestamp>-<tupleID>
)

// Persister takes care of persisting in-flight tuples and computation state
type Persister interface {
	Close()
	CommitComputation(compID s.StreamID, in *s.Tuple, out []*s.Tuple) error
	GetSnapshot(compID s.StreamID) (*s.ComputationSnapshot, error)
	ApplySnapshot(compID s.StreamID, snapshot *s.ComputationSnapshot) error
	LinearizerStore
	StreamBuffer
	SentTracker
	ReceivedTracker
}

type StreamBuffer interface {
	Insert1(compID s.StreamID, bufID string, t *s.Tuple) error
	ReadBuffer1(compID s.StreamID, bufID string, from time.Time, to time.Time, tupCh chan *s.Tuple, readCompleted chan struct{})
}

// SentTracker deletes production entries that have been ACKed from the DB
type SentTracker interface {
	SentSuccessfuly(s.StreamID, *s.Tuple) error
}

// MultiSentTracker enables notifying multiple SentTrackers of a successfuly
// sent tuple
type MultiSentTracker struct {
	trackers []SentTracker
}

func (st MultiSentTracker) SentSuccessfuly(compID s.StreamID, t *s.Tuple) error {
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
	PersistReceivedTuples(s.StreamID, []*s.Tuple) error
	GetRecentReceived(s.StreamID) ([]string, error)
	ReceivedAlready(s.StreamID, *s.Tuple) (bool, error)
	// PruneOlderThan
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
func (p *LevelDBPersister) CommitComputation(compID s.StreamID, in *s.Tuple, out []*s.Tuple) error {
	batch := new(leveldb.Batch)
	// mark incoming tuple as received
	log.Println("[persister] Committing tuple", *in, ", productions:", out)
	// batch.Put([]byte(fmt.Sprintf(receivedKeyFormat, compID, in.ID)), nil)
	lastTimestamp, err := json.Marshal(in.Timestamp)
	if err != nil {
		return err
	}
	batch.Put([]byte(fmt.Sprintf("%s-last", compID)), lastTimestamp)

	// save productions
	for _, t := range out {
		serialized, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("[persister] Error marshalling tuple %v: %s", t, err)
		}
		key := []byte(fmt.Sprintf(productionsKeyFormat, compID, t.Timestamp.UnixNano(), t.ID))
		batch.Put(key, []byte(serialized))
	}
	return p.db.Write(batch, nil)
}

// GetSnapshot returns the snapshot of the computation's persisted state
func (p *LevelDBPersister) GetSnapshot(compID s.StreamID) (*s.ComputationSnapshot, error) {
	dbSnapshot, err := p.db.GetSnapshot()
	defer dbSnapshot.Release()
	if err != nil {
		return nil, fmt.Errorf("[persister] Error getting snapshot: %s", err)
	}
	var snapshot s.ComputationSnapshot
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
		return nil, fmt.Errorf("[persister] error iterating: %s", err)
	}

	// get produced tuples
	snapshot.Produced = make([]*s.Tuple, 0)
	// keyPrefix = fmt.Sprintf(productionsKeyFormat, compID, "")
	keyPrefix = fmt.Sprintf("%s-p-", compID)
	iter2 := dbSnapshot.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter2.Release()
	for iter2.Next() {
		var tuple s.Tuple
		err := json.Unmarshal(iter2.Value(), &tuple)
		if err != nil {
			return nil, fmt.Errorf("[persister] unmarshalling produced: %s", err)
		}
		snapshot.Produced = append(snapshot.Produced, &tuple)
	}
	err = iter2.Error()
	if err != nil {
		return nil, fmt.Errorf("[persister] iterating produced: %s", err)
	}

	// get input buffer tuples
	snapshot.InputBuffer = make([]*s.Tuple, 0)
	keyPrefix = fmt.Sprintf("%s-i-", compID)
	iter3 := dbSnapshot.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter3.Release()
	for iter3.Next() {
		var tuple s.Tuple
		err := json.Unmarshal(iter3.Value(), &tuple)
		if err != nil {
			return nil, fmt.Errorf("[persister] unmarshalling produced: %s", err)
		}
		snapshot.InputBuffer = append(snapshot.InputBuffer, &tuple)
	}
	err = iter3.Error()
	if err != nil {
		return nil, fmt.Errorf("[persister] iterating input buffer: %s", err)
	}

	var lastTimestampUnmarshalled time.Time
	lastTimestamp, err := dbSnapshot.Get([]byte(fmt.Sprintf("%s-last", compID)), nil)
	if err != nil {
		// return nil, fmt.Errorf("[persister] reading last timestamp: %s", err)
		err = nil // FIXME sigh...
	} else {
		err = json.Unmarshal(lastTimestamp, &lastTimestampUnmarshalled)
		if err != nil {
			return nil, fmt.Errorf("[persister] reading last timestamp: %s", err)
		}
	}
	snapshot.LastTimestamp = lastTimestampUnmarshalled
	return &snapshot, err
}

// ApplySnapshot applies the snapshot of the computation's persisted state
func (p *LevelDBPersister) ApplySnapshot(compID s.StreamID, snapshot *s.ComputationSnapshot) error {
	batch := new(leveldb.Batch)
	log.Println("[persister] Applying snapshot", snapshot)

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
			return fmt.Errorf("[persister] Error marshalling tuple %v: %s", t, err)
		}
		key := []byte(fmt.Sprintf(productionsKeyFormat, compID, t.Timestamp.UnixNano(), t.ID))
		batch.Put(key, []byte(serialized))
	}

	// save input buffer
	for _, t := range snapshot.InputBuffer {
		serialized, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("[persister] Error marshalling tuple %v: %s", t, err)
		}
		key := []byte(fmt.Sprintf(inKeyFormat, compID, t.Timestamp.UnixNano(), t.ID))
		batch.Put(key, []byte(serialized))
	}

	// save last timestamp
	serialized, err := json.Marshal(snapshot.LastTimestamp)
	if err != nil {
		return fmt.Errorf("[persister] Error marshalling time %s", err)
	}
	batch.Put([]byte(fmt.Sprintf("%s-last", compID)), serialized)

	return p.db.Write(batch, nil)
}

// PersistReceivedTuples save the info about which tuples we've already seen
func (p *LevelDBPersister) PersistReceivedTuples(comp s.StreamID, tuples []*s.Tuple) error {
	batch := new(leveldb.Batch)
	for _, t := range tuples {
		batch.Put([]byte(fmt.Sprintf(receivedKeyFormat, comp, t.ID)), nil)
	}
	return p.db.Write(batch, nil)
}

// GetRecentReceived returns IDs of tuples we have recently received
func (p *LevelDBPersister) GetRecentReceived(comp s.StreamID) ([]string, error) {
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
func (p *LevelDBPersister) ReceivedAlready(comp s.StreamID, t *s.Tuple) (bool, error) {
	key := fmt.Sprintf(receivedKeyFormat, comp, t.ID)
	received, err := p.db.Has([]byte(key), nil)
	return received, err
}

// SentSuccessfuly deletes the production from the DB after it's been ACKed
func (p *LevelDBPersister) SentSuccessfuly(compID s.StreamID, t *s.Tuple) error {
	key := []byte(fmt.Sprintf(productionsKeyFormat, compID, t.Timestamp.UnixNano(), t.ID))
	return p.db.Delete(key, nil)
}

// Insert inserts a received tuple into the ordered queue for a computation
func (p *LevelDBPersister) Insert(compID s.StreamID, t *s.Tuple) error {
	serialized, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("[persister] Error marshalling tuple %v: %s", t, err)
	}
	key := []byte(fmt.Sprintf(inKeyFormat, compID, t.Timestamp.UnixNano(), t.ID))
	err = p.db.Put(key, []byte(serialized), &opt.WriteOptions{Sync: false})
	if err != nil {
		return fmt.Errorf("[persister] Error persisting tuple %v: %s", t, err)
	}
	log.Println("[persister] persisted tuple", string(key), t)
	return nil
}

// ReadBuffer returns a piece of the input buffer between specified timestamps
func (p *LevelDBPersister) ReadBuffer(compID s.StreamID, from time.Time, to time.Time) ([]*s.Tuple, error) {
	var tups []*s.Tuple
	start := []byte(fmt.Sprintf("%s-i-%d", compID, from.UnixNano()))
	limit := []byte(fmt.Sprintf("%s-i-%d", compID, to.UnixNano()))
	log.Println("reading0 from, to", string(start), string(limit))
	iter := p.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	for iter.Next() {
		var tuple s.Tuple
		err := json.Unmarshal(iter.Value(), &tuple)
		if err != nil {
			return nil, fmt.Errorf("[persister] unmarshalling produced: %s", err)
		}
		tups = append(tups, &tuple)
	}
	if len(tups) == 0 {
		log.Println("SOMETHING FUNNY")
		iter := p.db.NewIterator(util.BytesPrefix(start), nil)
		iter.Next()
		log.Println(iter.Value())
	}
	return tups, nil
}

// type StreamIterator interface {
// 	Upto(time.Time) chan *s.Tuple
// }

// Insert inserts a received tuple into the ordered queue for a computation
func (p *LevelDBPersister) Insert1(compID s.StreamID, bufID string, t *s.Tuple) error {
	serialized, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("[persister] Error marshalling tuple %v: %s", t, err)
	}
	key := []byte(fmt.Sprintf("%s-%s-%d", compID, bufID, t.Timestamp.UnixNano(), t.ID))
	err = p.db.Put(key, []byte(serialized), &opt.WriteOptions{Sync: false})
	if err != nil {
		return fmt.Errorf("[persister] Error persisting tuple %v: %s", t, err)
	}
	log.Println("[persister] persisted tuple", string(key), t)
	return nil
}

// ReadBuffer returns a piece of the input buffer between specified timestamps
func (p *LevelDBPersister) ReadBuffer1(compID s.StreamID, bufID string,
	from time.Time, to time.Time, tupCh chan *s.Tuple,
	readCompletedCh chan struct{}) { // FIXME: add errCh
	start := []byte(fmt.Sprintf("%s-%s-%d", compID, bufID, from.UnixNano()))
	limit := []byte(fmt.Sprintf("%s-%s-%d", compID, bufID, to.UnixNano()+1))
	go func() {
		log.Println("reading from, to", string(start), string(limit))
		iter := p.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
		for iter.Next() {
			var tuple s.Tuple
			err := json.Unmarshal(iter.Value(), &tuple)
			log.Println("read tuple", tuple)
			if err != nil {
				log.Println("ERRRROR", err)
				// errCh <- fmt.Errorf("[persister] unmarshalling produced: %s", err)
			}
			tupCh <- &tuple
		}
		readCompletedCh <- struct{}{}
	}()
	return
}

// type StreamIterator interface {
// 	Upto(time.Time) chan *s.Tuple
// }
