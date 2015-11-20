package dagger

import (
	"log"
)

// Linearizer buffers records and forwards them to the next RecordProcessor sorted
// by timestamp, while making sure all the records in a certain time frame have
// already arrived
type Linearizer struct {
	compID     StreamID
	store      StreamBuffer
	lwmTracker LWMTracker
	LWM        Timestamp
	lwmCh      chan Timestamp
	startAtLWM Timestamp
	ltp        LinearizedRecordProcessor
	tmpT       chan *Record
}

// NewLinearizer creates a new linearizer for a certain computation
func NewLinearizer(compID StreamID, store StreamBuffer, lwmTracker LWMTracker) *Linearizer {
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
	err := l.store.Insert(l.compID, "i", t)
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
	fromCh := make(chan Timestamp)
	toCh := make(chan Timestamp)
	recs := make(chan *Record)
	errc := make(chan error)

	go MovingLimitRead(l.store, l.compID, "i", fromCh, toCh, recs, errc)
	fromLWM := l.startAtLWM
	fromCh <- fromLWM
	for {
		select {
		case toLWM := <-l.lwmCh:
			t := <-l.tmpT // FIXME: remove
			if toLWM <= fromLWM {
				log.Println("LWM not increased", t)
			} else {
				log.Printf("PROCESSING as result of %s", t)
				toCh <- toLWM
			}
		case r := <-recs:
			l.ltp.ProcessRecordLinearized(r)
			fromLWM := r.LWM
			fromCh <- fromLWM + 1
		case err := <-errc:
			log.Println("PERSISTER ERROR", err) // FIXME: propagate
		}
	}
}
