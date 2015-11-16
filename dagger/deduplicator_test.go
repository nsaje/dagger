package dagger

import (
	"sync"
	"testing"

	"github.com/nsaje/dagger/s"
)

type inmemRecordTracker struct {
	set map[string]struct{}
	sync.RWMutex
}

func (tt *inmemRecordTracker) PersistReceivedRecords(compID s.StreamID, records []*s.Record) error {
	tt.Lock()
	defer tt.Unlock()
	for _, r := range records {
		tt.set[r.ID] = struct{}{}
	}
	return nil
}

func (tt *inmemRecordTracker) ReceivedAlready(compID s.StreamID, t *s.Record) (bool, error) {
	tt.RLock()
	defer tt.RUnlock()
	_, found := tt.set[t.ID]
	return found, nil
}

func (tt *inmemRecordTracker) GetRecentReceived(s.StreamID) ([]string, error) {
	received := make([]string, 0, len(tt.set))
	for k := range tt.set {
		received = append(received, k)
	}
	return received, nil
}

func TestDeduplicate(t *testing.T) {
	recordTracker := &inmemRecordTracker{set: make(map[string]struct{})}
	recordTracker.PersistReceivedRecords(s.StreamID("test"), []*s.Record{&s.Record{ID: "0"}})

	deduplicator, _ := NewDeduplicator(s.StreamID("test"), recordTracker)

	recs := []*s.Record{
		&s.Record{ID: "0"},
		&s.Record{ID: "1"},
		&s.Record{ID: "1"},
		&s.Record{ID: "2"},
	}

	recordTracker.PersistReceivedRecords("test", recs)

	var actual []*s.Record
	for _, rec := range recs {
		if seen, _ := deduplicator.Seen(rec); !seen {
			actual = append(actual, rec)
		}
	}
	if len(actual) != 2 {
		t.Errorf("Duplicate records: %v", actual)
	}
}
