package dagger

import (
	"github.com/willf/bloom"

	"bitbucket.org/nsaje/dagger/structs"
)

// Deduplicator throws away duplicate tuples (and ACKs their senders)
type Deduplicator interface {
	Deduplicate(chan *structs.Tuple) chan *structs.Tuple
}

type dedup struct {
	persister Persister
	filter    *bloom.BloomFilter
}

// NewDeduplicator initializes a new deduplicator
func NewDeduplicator(persister Persister) Deduplicator {
	return &dedup{
		persister: persister,
		filter:    bloom.New(20000, 5),
	}
}

func (d *dedup) Deduplicate(input chan *structs.Tuple) chan *structs.Tuple {
	output := make(chan *structs.Tuple)
	go func() {
		for t := range input {
			var seen bool
			if seen = d.filter.TestAndAddString(t.ID); seen {
				// we have probably seen it before, but we must check the DB
				seen = d.persister.ReceivedAlready(t)
			}
			if !seen {
				output <- t
			}
		}
	}()
	return output
}
