package dagger

import (
	"log"
	"time"

	"github.com/nsaje/dagger/s"
)

// LinearizerStore represents a sorted persistent buffer of tuples
type LinearizerStore interface {
	Insert(compID s.StreamID, t *s.Tuple) error
	ReadBuffer(compID s.StreamID, from time.Time, to time.Time) ([]*s.Tuple, error)
}

// Linearizer buffers tuples and forwards them to the next TupleProcessor sorted
// by timestamp, while making sure all the tuples in a certain time frame have
// already arrived
type Linearizer struct {
	compID     s.StreamID
	store      LinearizerStore
	lwmTracker LWMTracker
	LWM        time.Time
	lwmCh      chan time.Time
	startAtLWM time.Time
	ltp        LinearizedTupleProcessor
	tmpT       chan *s.Tuple
}

// NewLinearizer creates a new linearizer for a certain computation
func NewLinearizer(compID s.StreamID, store LinearizerStore, lwmTracker LWMTracker) *Linearizer {
	return &Linearizer{
		compID:     compID,
		store:      store,
		lwmTracker: lwmTracker,
		lwmCh:      make(chan time.Time),
		tmpT:       make(chan *s.Tuple),
	}
}

// SetProcessor sets the processor that received linearized tuiples
func (l *Linearizer) SetProcessor(ltp LinearizedTupleProcessor) {
	l.ltp = ltp
}

// SetStartLWM sets the LWM at which we start processing
func (l *Linearizer) SetStartLWM(time time.Time) {
	l.startAtLWM = time
}

// ProcessTuple inserts the tuple into the buffer sorted by timestamps
func (l *Linearizer) ProcessTuple(t *s.Tuple) error {
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
	fromLWM := l.startAtLWM
	for toLWM := range l.lwmCh {
		t := <-l.tmpT
		if !toLWM.After(fromLWM) {
			log.Println("LWM not increased", t)
			continue
		}
		tups, _ := l.store.ReadBuffer(l.compID, fromLWM, toLWM)
		log.Printf("PROCESSING as result of %s", t)
		for _, t := range tups {
			log.Println(t)
		}
		for _, t := range tups {
			l.ltp.ProcessTupleLinearized(t)
		}
		fromLWM = toLWM
	}
}
