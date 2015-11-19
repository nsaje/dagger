package dagger

import (
	"sync"
	"testing"

	
)

type inmemRecordTracker struct {
	set map[string]struct{}
	sync.RWMutex
}

func (tt *inmemRecordTracker) PersistReceivedRecords(compID StreamID, records []*Record) error {
	tt.Lock()
	defer tt.Unlock()
	for _, r := range records {
		tt.set[r.ID] = struct{}{}
	}
	return nil
}

func (tt *inmemRecordTracker) ReceivedAlready(compID StreamID, t *Record) (bool, error) {
	tt.RLock()
	defer tt.RUnlock()
	_, found := tt.set[t.ID]
	return found, nil
}

func (tt *inmemRecordTracker) GetRecentReceived(StreamID) ([]string, error) {
	received := make([]string, 0, len(tt.set))
	for k := range tt.set {
		received = append(received, k)
	}
	return received, nil
}

func TestDeduplicate(t *testing.T) {
	recordTracker := &inmemRecordTracker{set: make(map[string]struct{})}
	recordTracker.PersistReceivedRecords(StreamID("test"), []*Record{&Record{ID: "0"}})

	deduplicator, _ := NewDeduplicator(StreamID("test"), recordTracker)

	recs := []*Record{
		&Record{ID: "0"},
		&Record{ID: "1"},
		&Record{ID: "1"},
		&Record{ID: "2"},
	}

	recordTracker.PersistReceivedRecords("test", recs)

	var actual []*Record
	for _, rec := range recs {
		if seen, _ := deduplicator.Seen(rec); !seen {
			actual = append(actual, rec)
		}
	}
	if len(actual) != 2 {
		t.Errorf("Duplicate records: %v", actual)
	}
}
