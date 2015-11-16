package dagger

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/nsaje/dagger/s"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/twinj/uuid"
)

const (
	receivedKeyFormat    = "%s-r-%s"    // <streamID>-r-<recordID>
	productionsKeyFormat = "%s-p-%d-%s" // <streamID>-p-<timestamp>-<recordID>
	inKeyFormat          = "%s-i-%d-%s" // <streamID>-i-<timestamp>-<recordID>
)

// Persister takes care of persisting in-flight records and computation state
type Persister interface {
	Close()
	CommitComputation(compID s.StreamID, in *s.Record, out []*s.Record) error
	GetSnapshot(compID s.StreamID) (*s.TaskSnapshot, error)
	ApplySnapshot(compID s.StreamID, snapshot *s.TaskSnapshot) error
	LinearizerStore
	StreamBuffer
	SentTracker
	ReceivedTracker
}

type StreamBuffer interface {
	Insert1(compID s.StreamID, bufID string, t *s.Record) error
	ReadBuffer1(compID s.StreamID, bufID string, from s.Timestamp, to s.Timestamp, tupCh chan *s.Record, readCompleted chan struct{})
}

// SentTracker deletes production entries that have been ACKed from the DB
type SentTracker interface {
	SentSuccessfuly(s.StreamID, *s.Record) error
}

// MultiSentTracker enables notifying multiple SentTrackers of a successfuly
// sent record
type MultiSentTracker struct {
	trackers []SentTracker
}

func (st MultiSentTracker) SentSuccessfuly(compID s.StreamID, t *s.Record) error {
	for _, tracker := range st.trackers {
		err := tracker.SentSuccessfuly(compID, t)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReceivedTracker persists info about which records we've already seen
type ReceivedTracker interface {
	PersistReceivedTuples(s.StreamID, []*s.Record) error
	GetRecentReceived(s.StreamID) ([]string, error)
	ReceivedAlready(s.StreamID, *s.Record) (bool, error)
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

// CommitComputation persists information about received and produced records
// atomically
func (p *LevelDBPersister) CommitComputation(compID s.StreamID, in *s.Record, out []*s.Record) error {
	batch := new(leveldb.Batch)
	// mark incoming record as received
	log.Println("[persister] Committing record", *in, ", productions:", out)
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
			return fmt.Errorf("[persister] Error marshalling record %v: %s", t, err)
		}
		key := []byte(fmt.Sprintf(productionsKeyFormat, compID, t.Timestamp, t.ID))
		batch.Put(key, []byte(serialized))
	}
	return p.db.Write(batch, nil)
}

// GetSnapshot returns the snapshot of the computation's persisted state
func (p *LevelDBPersister) GetSnapshot(compID s.StreamID) (*s.TaskSnapshot, error) {
	dbSnapshot, err := p.db.GetSnapshot()
	defer dbSnapshot.Release()
	if err != nil {
		return nil, fmt.Errorf("[persister] Error getting snapshot: %s", err)
	}
	var snapshot s.TaskSnapshot
	// get received records
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

	// get produced records
	snapshot.Produced = make([]*s.Record, 0)
	// keyPrefix = fmt.Sprintf(productionsKeyFormat, compID, "")
	keyPrefix = fmt.Sprintf("%s-p-", compID)
	iter2 := dbSnapshot.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter2.Release()
	for iter2.Next() {
		var record s.Record
		err := json.Unmarshal(iter2.Value(), &record)
		if err != nil {
			return nil, fmt.Errorf("[persister] unmarshalling produced: %s", err)
		}
		snapshot.Produced = append(snapshot.Produced, &record)
	}
	err = iter2.Error()
	if err != nil {
		return nil, fmt.Errorf("[persister] iterating produced: %s", err)
	}

	// get input buffer records
	snapshot.InputBuffer = make([]*s.Record, 0)
	keyPrefix = fmt.Sprintf("%s-i-", compID)
	iter3 := dbSnapshot.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter3.Release()
	for iter3.Next() {
		var record s.Record
		err := json.Unmarshal(iter3.Value(), &record)
		if err != nil {
			return nil, fmt.Errorf("[persister] unmarshalling produced: %s", err)
		}
		snapshot.InputBuffer = append(snapshot.InputBuffer, &record)
	}
	err = iter3.Error()
	if err != nil {
		return nil, fmt.Errorf("[persister] iterating input buffer: %s", err)
	}

	var lastTimestampUnmarshalled s.Timestamp
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
func (p *LevelDBPersister) ApplySnapshot(compID s.StreamID, snapshot *s.TaskSnapshot) error {
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
			return fmt.Errorf("[persister] Error marshalling record %v: %s", t, err)
		}
		key := []byte(fmt.Sprintf(productionsKeyFormat, compID, t.Timestamp, t.ID))
		batch.Put(key, []byte(serialized))
	}

	// save input buffer
	for _, t := range snapshot.InputBuffer {
		serialized, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("[persister] Error marshalling record %v: %s", t, err)
		}
		key := []byte(fmt.Sprintf(inKeyFormat, compID, t.Timestamp, t.ID))
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

// PersistReceivedTuples save the info about which records we've already seen
func (p *LevelDBPersister) PersistReceivedTuples(comp s.StreamID, records []*s.Record) error {
	batch := new(leveldb.Batch)
	for _, t := range records {
		batch.Put([]byte(fmt.Sprintf(receivedKeyFormat, comp, t.ID)), nil)
	}
	return p.db.Write(batch, nil)
}

// GetRecentReceived returns IDs of records we have recently received
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

// ReceivedAlready returns whether we've seen this record before
func (p *LevelDBPersister) ReceivedAlready(comp s.StreamID, t *s.Record) (bool, error) {
	key := fmt.Sprintf(receivedKeyFormat, comp, t.ID)
	received, err := p.db.Has([]byte(key), nil)
	return received, err
}

// SentSuccessfuly deletes the production from the DB after it's been ACKed
func (p *LevelDBPersister) SentSuccessfuly(compID s.StreamID, t *s.Record) error {
	key := []byte(fmt.Sprintf(productionsKeyFormat, compID, t.Timestamp, t.ID))
	return p.db.Delete(key, nil)
}

// Insert inserts a received record into the ordered queue for a computation
func (p *LevelDBPersister) Insert(compID s.StreamID, t *s.Record) error {
	serialized, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("[persister] Error marshalling record %v: %s", t, err)
	}
	key := []byte(fmt.Sprintf(inKeyFormat, compID, t.Timestamp, t.ID))
	err = p.db.Put(key, []byte(serialized), &opt.WriteOptions{Sync: false})
	if err != nil {
		return fmt.Errorf("[persister] Error persisting record %v: %s", t, err)
	}
	log.Println("[persister] persisted record", string(key), t)
	return nil
}

// ReadBuffer returns a piece of the input buffer between specified timestamps
func (p *LevelDBPersister) ReadBuffer(compID s.StreamID, from s.Timestamp, to s.Timestamp) ([]*s.Record, error) {
	var tups []*s.Record
	start := []byte(fmt.Sprintf("%s-i-%d", compID, from))
	limit := []byte(fmt.Sprintf("%s-i-%d", compID, to))
	log.Println("reading0 from, to", string(start), string(limit))
	iter := p.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	for iter.Next() {
		var record s.Record
		err := json.Unmarshal(iter.Value(), &record)
		if err != nil {
			return nil, fmt.Errorf("[persister] unmarshalling produced: %s", err)
		}
		tups = append(tups, &record)
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
// 	Upto(s.Timestamp) chan *s.Tuple
// }

// Insert inserts a received record into the ordered queue for a computation
func (p *LevelDBPersister) Insert1(compID s.StreamID, bufID string, t *s.Record) error {
	serialized, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("[persister] Error marshalling record %v: %s", t, err)
	}
	key := []byte(fmt.Sprintf("%s-%s-%d", compID, bufID, t.Timestamp, t.ID))
	err = p.db.Put(key, []byte(serialized), &opt.WriteOptions{Sync: false})
	if err != nil {
		return fmt.Errorf("[persister] Error persisting record %v: %s", t, err)
	}
	log.Println("[persister] persisted record", string(key), t)
	return nil
}

// ReadBuffer returns a piece of the input buffer between specified timestamps
func (p *LevelDBPersister) ReadBuffer1(compID s.StreamID, bufID string,
	from s.Timestamp, to s.Timestamp, tupCh chan *s.Record,
	readCompletedCh chan struct{}) { // FIXME: add errCh
	start := []byte(fmt.Sprintf("%s-%s-%d", compID, bufID, from))
	limit := []byte(fmt.Sprintf("%s-%s-%d", compID, bufID, to))
	go func() {
		log.Println("reading from, to", string(start), string(limit))
		iter := p.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
		for iter.Next() {
			var record s.Record
			err := json.Unmarshal(iter.Value(), &record)
			log.Println("read record", record)
			if err != nil {
				log.Println("ERRRROR", err)
				// errCh <- fmt.Errorf("[persister] unmarshalling produced: %s", err)
			}
			tupCh <- &record
		}
		readCompletedCh <- struct{}{}
	}()
	return
}

// type StreamIterator interface {
// 	Upto(s.Timestamp) chan *s.Tuple
// }
