package dagger

import (
	"fmt"
	"log"

	"bitbucket.org/nsaje/dagger/structs"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	receivedKeyFormat = "r-%s"
)

// Persister takes care of persisting in-flight tuples and computation state
type Persister interface {
	Close()
}

// TupleTracker persists info about which tuples we've already seen
type TupleTracker interface {
	PersistReceivedTuples(tuples []*structs.Tuple)
	ReceivedAlready(t *structs.Tuple) bool
}

// LevelDBPersister is built on top of LevelDB
type LevelDBPersister struct {
	db *leveldb.DB
}

// NewPersister initializes and returns a new Persister instance
func NewPersister(conf *Config) (*LevelDBPersister, error) {
	db, err := leveldb.OpenFile(conf.LevelDBFile, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDBPersister{db}, nil
}

func (p *LevelDBPersister) Close() {
	p.db.Close()
}

func (p *LevelDBPersister) PersistState() {
	return
}

func (p *LevelDBPersister) PersistReceivedTuples(tuples []*structs.Tuple) {
	batch := new(leveldb.Batch)
	for _, t := range tuples {
		batch.Put([]byte(fmt.Sprintf(receivedKeyFormat, t.ID)), nil)
	}
}

func (p *LevelDBPersister) ReceivedAlready(t *structs.Tuple) bool {
	received, err := p.db.Has([]byte(fmt.Sprintf(receivedKeyFormat, t.ID)), nil)
	if err != nil {
		log.Fatal("can't read from persister DB")
	}
	return received
}
