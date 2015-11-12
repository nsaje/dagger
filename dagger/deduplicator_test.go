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

func (tt *inmemTupleTracker) PersistReceivedTuples(compID s.StreamID, tuples []*s.Tuple) error {
	tt.Lock()
	defer tt.Unlock()
	for _, t := range tuples {
		tt.set[t.ID] = struct{}{}
	}
	return nil
}

func (tt *inmemTupleTracker) ReceivedAlready(compID s.StreamID, t *s.Tuple) (bool, error) {
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
	tupleTracker := &inmemTupleTracker{set: make(map[string]struct{})}
	tupleTracker.PersistReceivedTuples(s.StreamID("test"), []*s.Tuple{&s.Tuple{ID: "0"}})

	deduplicator, _ := NewDeduplicator(s.StreamID("test"), tupleTracker)

	tups := []*s.Tuple{
		&s.Tuple{ID: "0"},
		&s.Tuple{ID: "1"},
		&s.Tuple{ID: "1"},
		&s.Tuple{ID: "2"},
	}

	tupleTracker.PersistReceivedTuples("test", tups)

	var actual []*s.Tuple
	for _, tup := range tups {
		if seen, _ := deduplicator.Seen(tup); !seen {
			actual = append(actual, tup)
		}
	}
	if len(actual) != 2 {
		t.Errorf("Duplicate tuples: %v", actual)
	}
}
