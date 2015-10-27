package dagger

import (
	"log"
	"time"

	"github.com/nsaje/dagger/structs"
)

// LinearizerStore represents a sorted persistent buffer of tuples
type LinearizerStore interface {
	Insert(compID string, t *structs.Tuple) error
	ReadBuffer(compID string, from time.Time, to time.Time) ([]*structs.Tuple, error)
}

// Linearizer buffers tuples and forwards them to the next TupleProcessor sorted
// by timestamp, while making sure all the tuples in a certain time frame have
// already arrived
type Linearizer struct {
	compID     string
	store      LinearizerStore
	lwmTracker LWMTracker
	LWM        time.Time
	lwmCh      chan time.Time
	tmpT       chan *structs.Tuple
	next       TupleProcessor
}

// NewLinearizer creates a new linearizer for a certain computation
func NewLinearizer(compID string, store LinearizerStore, lwmTracker LWMTracker, next TupleProcessor) *Linearizer {
	return &Linearizer{
		compID:     compID,
		store:      store,
		next:       next,
		lwmTracker: lwmTracker,
		lwmCh:      make(chan time.Time),
		tmpT:       make(chan *structs.Tuple),
	}
}

// ProcessTuple inserts the tuple into the buffer sorted by timestamps
func (bh *Linearizer) ProcessTuple(t *structs.Tuple) error {
	err := bh.store.Insert(bh.compID, t)
	if err != nil {
		return err
	}

	// calculate low water mark (LWM)
	// LWM = min(oldest event in this computation, LWM across all publishers this
	// computation is subscribed to)
	err = bh.lwmTracker.ProcessTuple(t)
	if err != nil {
		return err
	}
	upstreamLWM := bh.lwmTracker.GetUpstreamLWM()
	bh.lwmCh <- upstreamLWM
	bh.tmpT <- t
	return nil
}

// StartForwarding starts a goroutine that forwards the tuples from the buffer
// to the next TupleProcessor when LWM tells us no more tuples will arrive in
// the forwarded time frame
func (bh *Linearizer) StartForwarding() {
	fromLWM := time.Time{}
	go func() {
		for toLWM := range bh.lwmCh {
			t := <-bh.tmpT
			if !toLWM.After(fromLWM) {
				log.Println("LWM not increased", t)
				continue
			}
			tups, _ := bh.store.ReadBuffer(bh.compID, fromLWM, toLWM)
			log.Printf("PROCESSING %s as result of ", t)
			for _, t := range tups {
				log.Println(t)
			}
			for _, t := range tups {
				bh.next.ProcessTuple(t)
			}
			fromLWM = toLWM
		}
	}()
}
