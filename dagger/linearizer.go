package dagger

import (
	"log"

	
)

// LinearizerStore represents a sorted persistent buffer of records
type LinearizerStore interface {
	Insert(compID StreamID, t *Record) error
	ReadBuffer(compID StreamID, from Timestamp, to Timestamp) ([]*Record, error)
}

// Linearizer buffers records and forwards them to the next RecordProcessor sorted
// by timestamp, while making sure all the records in a certain time frame have
// already arrived
type Linearizer struct {
	compID     StreamID
	store      LinearizerStore
	lwmTracker LWMTracker
	LWM        Timestamp
	lwmCh      chan Timestamp
	startAtLWM Timestamp
	ltp        LinearizedRecordProcessor
	tmpT       chan *Record
}

// NewLinearizer creates a new linearizer for a certain computation
func NewLinearizer(compID StreamID, store LinearizerStore, lwmTracker LWMTracker) *Linearizer {
	return &Linearizer{
		compID:     compID,
		store:      store,
		lwmTracker: lwmTracker,
		lwmCh:      make(chan Timestamp),
		tmpT:       make(chan *Record),
	}
}

// SetProcessor sets the processor that received linearized tuiples
func (l *Linearizer) SetProcessor(ltp LinearizedRecordProcessor) {
	l.ltp = ltp
}

// SetStartLWM sets the LWM at which we start processing
func (l *Linearizer) SetStartLWM(time Timestamp) {
	l.startAtLWM = time
}

// ProcessRecord inserts the record into the buffer sorted by timestamps
func (l *Linearizer) ProcessRecord(t *Record) error {
	err := l.store.Insert(l.compID, t)
	if err != nil {
		return err
	}

	// calculate low water mark (LWM)
	// LWM = min(oldest event in this computation, LWM across all publishers this
	// computation is subscribed to)
	err = l.lwmTracker.ProcessRecord(t)
	if err != nil {
		return err
	}
	upstreamLWM := l.lwmTracker.GetUpstreamLWM()
	l.lwmCh <- upstreamLWM
	l.tmpT <- t
	return nil
}

// StartForwarding starts a goroutine that forwards the records from the buffer
// to the next RecordProcessor when LWM tells us no more records will arrive in
// the forwarded time frame
func (l *Linearizer) StartForwarding() {
	fromLWM := l.startAtLWM
	for toLWM := range l.lwmCh {
		t := <-l.tmpT
		// if !toLWM.After(fromLWM) {
		if toLWM <= fromLWM {
			log.Println("LWM not increased", t)
			continue
		}
		recs, _ := l.store.ReadBuffer(l.compID, fromLWM, toLWM)
		log.Printf("PROCESSING as result of %s", t)
		for _, r := range recs {
			log.Println(r)
		}
		for _, r := range recs {
			l.ltp.ProcessRecordLinearized(r)
		}
		fromLWM = toLWM
	}
}
