package dagger

import (
	"sync"
	"testing"

	"github.com/nsaje/dagger/structs"
)

type inmemTupleTracker struct {
	set map[string]struct{}
	sync.RWMutex
}

func (tt *inmemTupleTracker) PersistReceivedTuples(compID string, tuples []*structs.Tuple) error {
	tt.Lock()
	defer tt.Unlock()
	for _, t := range tuples {
		tt.set[t.ID] = struct{}{}
	}
	return nil
}

func (tt *inmemTupleTracker) ReceivedAlready(compID string, t *structs.Tuple) (bool, error) {
	tt.RLock()
	defer tt.RUnlock()
	_, found := tt.set[t.ID]
	return found, nil
}

func (tt *inmemTupleTracker) GetRecentReceived(string) ([]string, error) {
	received := make([]string, 0, len(tt.set))
	for k := range tt.set {
		received = append(received, k)
	}
	return received, nil
}

func TestDeduplicate(t *testing.T) {
	tupleTracker := &inmemTupleTracker{set: make(map[string]struct{})}
	tupleTracker.PersistReceivedTuples("test", []*structs.Tuple{&structs.Tuple{ID: "0"}})

	deduplicator, _ := NewDeduplicator("test", tupleTracker)

	tups := []*structs.Tuple{
		&structs.Tuple{ID: "0"},
		&structs.Tuple{ID: "1"},
		&structs.Tuple{ID: "1"},
		&structs.Tuple{ID: "2"},
	}

	tupleTracker.PersistReceivedTuples("test", tups)

	var actual []*structs.Tuple
	for _, tup := range tups {
		if seen, _ := deduplicator.Seen(tup); !seen {
			actual = append(actual, tup)
		}
	}
	if len(actual) != 2 {
		t.Errorf("Duplicate tuples: %v", actual)
	}
}
