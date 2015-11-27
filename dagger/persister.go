package dagger

import (
	"encoding/json"
	"fmt"
	"log"
	"path"

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
	TaskPersister
	StreamBuffer
	SentTracker
	ReceivedTracker
}

// TaskPersister enables tasks to commit and restore their state
type TaskPersister interface {
	CommitComputation(compID StreamID, in *Record, out []*Record) error
	GetLastTimestamp(compID StreamID) (Timestamp, error)
	GetSnapshot(compID StreamID) ([]byte, error)
	ApplySnapshot(compID StreamID, snapshot []byte) error
}

// StreamBuffer persistently "buffers" records sorted by timestamp
type StreamBuffer interface {
	Insert(compID StreamID, bufID string, t *Record) error
	ReadBuffer(compID StreamID, bufID string, from Timestamp, to Timestamp, recCh chan<- *Record, errc chan<- error, readCompleted chan struct{})
}

// SentTracker deletes production entries that have been ACKed from the DB
type SentTracker interface {
	SentSuccessfuly(StreamID, *Record) error
}

// MultiSentTracker enables notifying multiple SentTrackers of a successfuly
// sent record
type MultiSentTracker struct {
	trackers []SentTracker
}

// SentSuccessfuly notifies multiple senttrackers of successfuly sent record
func (st MultiSentTracker) SentSuccessfuly(compID StreamID, t *Record) error {
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
	PersistReceivedRecords(StreamID, []*Record) error
	GetRecentReceived(StreamID) ([]string, error)
	ReceivedAlready(StreamID, *Record) (bool, error)
	// PruneOlderThan
}

// NewPersister initializes and returns a new Persister instance
func NewPersister(dir string) (Persister, error) {
	filename := path.Join(dir, "daggerDB-"+uuid.NewV4().String())
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
func (p *LevelDBPersister) CommitComputation(compID StreamID, in *Record, out []*Record) error {
	batch := new(leveldb.Batch)
	// mark incoming record as received
	log.Println("[persister] Committing record", in, ", productions:", out)
	// batch.Put([]byte(fmt.Sprintf(receivedKeyFormat, compID, in.ID)), nil)
	lastTimestamp, err := json.Marshal(in.Timestamp)
	if err != nil {
		return err
	}
	batch.Put([]byte(fmt.Sprintf("%s-last", compID)), lastTimestamp)

	// save productions
	for _, r := range out {
		serialized, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("[persister] Error marshalling record %v: %s", r, err)
		}
		key := []byte(fmt.Sprintf(productionsKeyFormat, compID, r.Timestamp, r.ID))
		batch.Put(key, []byte(serialized))
	}
	return p.db.Write(batch, nil)
}

func (p *LevelDBPersister) GetLastTimestamp(compID StreamID) (Timestamp, error) {
	val, err := p.db.Get([]byte(fmt.Sprintf("%s-last", compID)), nil)
	var ts Timestamp
	err = json.Unmarshal(val, &ts)
	return ts, err
}

// GetSnapshot returns the snapshot of the computation's persisted state
func (p *LevelDBPersister) GetSnapshot(compID StreamID) ([]byte, error) {
	dbSnapshot, err := p.db.GetSnapshot()
	defer dbSnapshot.Release()
	if err != nil {
		return nil, fmt.Errorf("[persister] Error getting snapshot: %s", err)
	}

	keyPrefix := compID
	batch := new(leveldb.Batch)
	iter := dbSnapshot.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()
	for iter.Next() {
		batch.Put(iter.Key(), iter.Value())
	}
	err = iter.Error()
	if err != nil {
		return nil, fmt.Errorf("[persister] error iterating: %s", err)
	}
	return batch.Dump(), nil
}

// ApplySnapshot applies the snapshot of the computation's persisted state
func (p *LevelDBPersister) ApplySnapshot(compID StreamID, snapshot []byte) error {
	batch := new(leveldb.Batch)
	log.Println("[persister] Applying snapshot") //, snapshot)

	// clear data for this computation
	keyPrefix := compID
	iter := p.db.NewIterator(util.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()
	for iter.Next() {
		batch.Delete(iter.Key())
	}
	err := p.db.Write(batch, nil)
	if err != nil {
		return err
	}
	batch.Reset()
	err = batch.Load(snapshot)
	if err != nil {
		return err
	}
	return p.db.Write(batch, nil)
}

// PersistReceivedRecords save the info about which records we've already seen
func (p *LevelDBPersister) PersistReceivedRecords(comp StreamID, records []*Record) error {
	batch := new(leveldb.Batch)
	for _, r := range records {
		batch.Put([]byte(fmt.Sprintf(receivedKeyFormat, comp, r.ID)), nil)
	}
	return p.db.Write(batch, nil)
}

// GetRecentReceived returns IDs of records we have recently received
func (p *LevelDBPersister) GetRecentReceived(comp StreamID) ([]string, error) {
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
func (p *LevelDBPersister) ReceivedAlready(comp StreamID, t *Record) (bool, error) {
	key := fmt.Sprintf(receivedKeyFormat, comp, t.ID)
	received, err := p.db.Has([]byte(key), nil)
	return received, err
}

// SentSuccessfuly deletes the production from the DB after it's been ACKed
func (p *LevelDBPersister) SentSuccessfuly(compID StreamID, t *Record) error {
	key := []byte(fmt.Sprintf(productionsKeyFormat, compID, t.Timestamp, t.ID))
	return p.db.Delete(key, nil)
}

// Insert inserts a received record into the ordered queue for a computation
func (p *LevelDBPersister) Insert(compID StreamID, bufID string, t *Record) error {
	serialized, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("[persister] Error marshalling record %v: %s", t, err)
	}
	key := []byte(fmt.Sprintf("%s-%s-%d-%s", compID, bufID, t.Timestamp, t.ID))
	err = p.db.Put(key, []byte(serialized), &opt.WriteOptions{Sync: false})
	if err != nil {
		return fmt.Errorf("[persister] Error persisting record %v: %s", t, err)
	}
	log.Println("[persister] persisted record", string(key), t)
	return nil
}

// ReadBuffer returns a piece of the input buffer between specified timestamps
func (p *LevelDBPersister) ReadBuffer(compID StreamID, bufID string,
	from Timestamp, to Timestamp, recCh chan<- *Record, errc chan<- error,
	readCompletedCh chan struct{}) {
	start := []byte(fmt.Sprintf("%s-%s-%d", compID, bufID, from))
	limit := []byte(fmt.Sprintf("%s-%s-%d", compID, bufID, to))
	go func() {
		log.Println("reading from, to", string(start), string(limit))
		iter := p.db.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
		for iter.Next() {
			var record Record
			err := json.Unmarshal(iter.Value(), &record)
			if err != nil {
				errc <- fmt.Errorf("[persister] unmarshalling err: %s", err)
			}
			recCh <- &record
		}
		readCompletedCh <- struct{}{}
	}()
	return
}

// MovingLimitRead performs asynchronous reads of the stream buffer. While the read
// is in progress, the from and to limits can be updated multiple times and the latest
// values will be used in the next read when the first completes.
func MovingLimitRead(p StreamBuffer, streamID StreamID, bufName string, from <-chan Timestamp, to <-chan Timestamp, recs chan<- *Record, errc chan error) {
	var newDataReady, readInProgress bool
	var newTo, newFrom Timestamp
	readCompleted := make(chan struct{})

	for {
		select {
		case newTo = <-to:
			newDataReady = true
		case newFrom = <-from:
		case <-readCompleted:
			readInProgress = false
		}
		if newDataReady && !readInProgress {
			log.Println("[persister] reading:", newFrom, newTo+1)
			newDataReady = false
			readInProgress = true
			// to + 1 so the 'to' is included
			p.ReadBuffer(streamID, bufName, newFrom, newTo+1, recs, errc, readCompleted)
			newFrom = newTo + 1
		}
	}
}
