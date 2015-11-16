package dagger

import (
	"sync"
	"testing"

	"github.com/nsaje/dagger/s"
)

type inmemTupleTracker struct {
	set map[string]struct{}
	sync.RWMutex
}

func (tt *inmemTupleTracker) PersistReceivedTuples(compID s.StreamID, records []*s.Record) error {
	tt.Lock()
	defer tt.Unlock()
	for _, t := range records {
		tt.set[t.ID] = struct{}{}
	}
	return nil
}

func (tt *inmemTupleTracker) ReceivedAlready(compID s.StreamID, t *s.Record) (bool, error) {
	tt.RLock()
	defer tt.RUnlock()
	_, found := tt.set[t.ID]
	return found, nil
}

func (tt *inmemTupleTracker) GetRecentReceived(s.StreamID) ([]string, error) {
	received := make([]string, 0, len(tt.set))
	for k := range tt.set {
		received = append(received, k)
	}
	return received, nil
}

func TestDeduplicate(t *testing.T) {
	recordTracker := &inmemTupleTracker{set: make(map[string]struct{})}
	recordTracker.PersistReceivedTuples(s.StreamID("test"), []*s.Record{&s.Record{ID: "0"}})

	deduplicator, _ := NewDeduplicator(s.StreamID("test"), recordTracker)

	tups := []*s.Record{
		&s.Record{ID: "0"},
		&s.Record{ID: "1"},
		&s.Record{ID: "1"},
		&s.Record{ID: "2"},
	}

	recordTracker.PersistReceivedTuples("test", tups)

	var actual []*s.Record
	for _, tup := range tups {
		if seen, _ := deduplicator.Seen(tup); !seen {
			actual = append(actual, tup)
		}
	}
	if len(actual) != 2 {
		t.Errorf("Duplicate records: %v", actual)
	}
}
