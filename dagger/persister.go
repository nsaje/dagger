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
	ReceivedAlready(t *structs.Tuple) bool
}

type leveldbPersister struct {
	db *leveldb.DB
}

// NewPersister initializes and returns a new Persister instance
func NewPersister(conf *Config) (Persister, error) {
	db, err := leveldb.OpenFile(conf.LevelDBFile, nil)
	if err != nil {
		return nil, err
	}
	return &leveldbPersister{db}, nil
}

func (p *leveldbPersister) Close() {
	p.db.Close()
}

func (p *leveldbPersister) PersistState() {
	return
}

func (p *leveldbPersister) PersistReceivedTuples(tuples []*structs.Tuple) {
	batch := new(leveldb.Batch)
	for _, t := range tuples {
		batch.Put([]byte(fmt.Sprintf(receivedKeyFormat, t.ID)), nil)
	}
}

func (p *leveldbPersister) ReceivedAlready(t *structs.Tuple) bool {
	received, err := p.db.Has([]byte(fmt.Sprintf(receivedKeyFormat, t.ID)), nil)
	if err != nil {
		log.Fatal("can't read from persister DB")
	}
	return received
}
