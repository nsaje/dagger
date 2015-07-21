package dagger

import (
	"log"

	"github.com/willf/bloom"

	"bitbucket.org/nsaje/dagger/structs"
)

// Deduplicator throws away duplicate tuples (and ACKs their senders)
type Deduplicator interface {
	Deduplicate(chan *structs.Tuple) chan *structs.Tuple
}

type dedup struct {
	computation  string
	tupleTracker ReceivedTracker
	filter       *bloom.BloomFilter
}

// NewDeduplicator initializes a new deduplicator
func NewDeduplicator(computation string, tupleTracker ReceivedTracker) (Deduplicator, error) {
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

func (d *dedup) Deduplicate(input chan *structs.Tuple) chan *structs.Tuple {
	output := make(chan *structs.Tuple)
	go func() {
		for t := range input {
			var seen bool
			var err error
			if seen = d.filter.TestAndAddString(t.ID); seen {
				// we have probably seen it before, but we must check the DB
				seen, err = d.tupleTracker.ReceivedAlready(d.computation, t)
				if err != nil {
					log.Panic("error reading DB") // FIXME
				}
			}
			if !seen {
				output <- t
			}
		}
		close(output)
	}()
	return output
}
