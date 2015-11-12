package dagger

import (
	"fmt"

	"github.com/nsaje/dagger/s"
	"github.com/willf/bloom"
)

// Deduplicator throws away duplicate tuples (and ACKs their senders)
type Deduplicator interface {
	Seen(t *s.Tuple) (bool, error)
}

type dedup struct {
	computation  s.StreamID
	tupleTracker ReceivedTracker
	filter       *bloom.BloomFilter
}

// NewDeduplicator initializes a new deduplicator
func NewDeduplicator(computation s.StreamID, tupleTracker ReceivedTracker) (Deduplicator, error) {
	dd := &dedup{
		computation:  computation,
		tupleTracker: tupleTracker,
		filter:       bloom.New(20000, 5),
	}
	recent, err := tupleTracker.GetRecentReceived(computation)
	if err != nil {
		return nil, err
	}

	for _, tupleID := range recent {
		dd.filter.AddString(tupleID)
	}
	return dd, nil
}

func (d *dedup) Seen(t *s.Tuple) (bool, error) {
	var seen bool
	var err error
	if seen = d.filter.TestAndAddString(t.ID); seen {
		// we have probably seen it before, but we must check the DB
		seen, err = d.tupleTracker.ReceivedAlready(d.computation, t)
		if err != nil {
			return true, fmt.Errorf("Error deduplicating: %s", err)
		}
	}
	return seen, nil
}
