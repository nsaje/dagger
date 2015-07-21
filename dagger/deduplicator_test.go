package dagger

import (
	"fmt"
	"sync"
	"testing"

	"bitbucket.org/nsaje/dagger/structs"
)

type inmemTupleTracker struct {
	set map[string]struct{}
	sync.RWMutex
}

func (tt *inmemTupleTracker) PersistReceivedTuples(tuples []*structs.Tuple) {
	tt.Lock()
	defer tt.Unlock()
	for _, t := range tuples {
		fmt.Println("setting for ", t.ID)
		tt.set[t.ID] = struct{}{}
	}
}

func (tt *inmemTupleTracker) ReceivedAlready(t *structs.Tuple) bool {
	tt.RLock()
	defer tt.RUnlock()
	_, found := tt.set[t.ID]
	fmt.Println("returning for ", t.ID, found)
	return found
}

func TestDeduplicate(t *testing.T) {
	tupleTracker := &inmemTupleTracker{set: make(map[string]struct{})}
	tupleTracker.PersistReceivedTuples([]*structs.Tuple{&structs.Tuple{ID: "0"}})

	deduplicator := NewDeduplicator(tupleTracker)

	in := make(chan *structs.Tuple, 3)
	out := deduplicator.Deduplicate(in)

	tups := []*structs.Tuple{
		&structs.Tuple{ID: "0"},
		&structs.Tuple{ID: "1"},
		&structs.Tuple{ID: "1"},
		&structs.Tuple{ID: "2"},
	}
	for _, t := range tups {
		in <- t
	}
	close(in)
	tupleTracker.PersistReceivedTuples(tups)

	var actual []*structs.Tuple
	for t := range out {
		actual = append(actual, t)
	}
	if len(actual) != 2 {
		t.Error("Duplicate tuples: ", actual)
	}
}
