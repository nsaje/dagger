package dagger

import (
	"fmt"

	"bitbucket.org/nsaje/dagger/structs"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/twinj/uuid"
)

const (
	receivedKeyFormat = "r-%s-%s"
)

// Persister takes care of persisting in-flight tuples and computation state
type Persister interface {
	Close()
	ReceivedTracker
	StatePersister
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
	db, err := leveldb.OpenFile(uuid.NewV4().String(), nil)
	if err != nil {
		return nil, err
	}
	return &LevelDBPersister{db}, nil
}

// LevelDBPersister is built on top of LevelDB
type LevelDBPersister struct {
	db *leveldb.DB
}

// Close the persister
func (p *LevelDBPersister) Close() {
	p.db.Close()
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
