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

	Linearized chan *structs.Tuple
}

// NewLinearizer creates a new linearizer for a certain computation
func NewLinearizer(compID string, store LinearizerStore, lwmTracker LWMTracker) *Linearizer {
	return &Linearizer{
		compID:     compID,
		store:      store,
		lwmTracker: lwmTracker,
		lwmCh:      make(chan time.Time),
		tmpT:       make(chan *structs.Tuple),
		Linearized: make(chan *structs.Tuple),
	}
}

// ProcessTuple inserts the tuple into the buffer sorted by timestamps
func (l *Linearizer) ProcessTuple(t *structs.Tuple) error {
	err := l.store.Insert(l.compID, t)
	if err != nil {
		return err
	}

	// calculate low water mark (LWM)
	// LWM = min(oldest event in this computation, LWM across all publishers this
	// computation is subscribed to)
	err = l.lwmTracker.ProcessTuple(t)
	if err != nil {
		return err
	}
	upstreamLWM := l.lwmTracker.GetUpstreamLWM()
	l.lwmCh <- upstreamLWM
	l.tmpT <- t
	return nil
}

// StartForwarding starts a goroutine that forwards the tuples from the buffer
// to the next TupleProcessor when LWM tells us no more tuples will arrive in
// the forwarded time frame
func (l *Linearizer) StartForwarding() {
	fromLWM := time.Time{}
	for toLWM := range l.lwmCh {
		t := <-l.tmpT
		if !toLWM.After(fromLWM) {
			log.Println("LWM not increased", t)
			continue
		}
		tups, _ := l.store.ReadBuffer(l.compID, fromLWM, toLWM)
		log.Printf("PROCESSING %s as result of ", t)
		for _, t := range tups {
			log.Println(t)
		}
		for _, t := range tups {
			l.Linearized <- t
		}
		fromLWM = toLWM
	}
}
