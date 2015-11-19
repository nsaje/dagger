package dagger

import (
	"fmt"

	
	"github.com/willf/bloom"
)

// Deduplicator throws away duplicate records (and ACKs their senders)
type Deduplicator interface {
	Seen(t *Record) (bool, error)
}

type dedup struct {
	computation  StreamID
	recordTracker ReceivedTracker
	filter       *bloom.BloomFilter
}

// NewDeduplicator initializes a new deduplicator
func NewDeduplicator(computation StreamID, recordTracker ReceivedTracker) (Deduplicator, error) {
	dd := &dedup{
		computation:  computation,
		recordTracker: recordTracker,
		filter:       bloom.New(20000, 5),
	}
	recent, err := recordTracker.GetRecentReceived(computation)
	if err != nil {
		return nil, err
	}

	for _, recordID := range recent {
		dd.filter.AddString(recordID)
	}
	return dd, nil
}

func (d *dedup) Seen(t *Record) (bool, error) {
	var seen bool
	var err error
	if seen = d.filter.TestAndAddString(t.ID); seen {
		// we have probably seen it before, but we must check the DB
		seen, err = d.recordTracker.ReceivedAlready(d.computation, t)
		if err != nil {
			return true, fmt.Errorf("Error deduplicating: %s", err)
		}
	}
	return seen, nil
}
