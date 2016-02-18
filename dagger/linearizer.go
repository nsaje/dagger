package dagger

import (
	log "github.com/Sirupsen/logrus"
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
	stopCh     chan struct{}
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
		stopCh:     make(chan struct{}),
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

// Stop stops the linearizer
func (l *Linearizer) Stop() {
	l.stopCh <- struct{}{}
	<-l.stopCh
}

// Run forwards the records from the buffer
// to the next RecordProcessor when LWM tells us no more records will arrive in
// the forwarded time frame
func (l *Linearizer) Run(errc chan error) {
	fromCh := make(chan Timestamp)
	toCh := make(chan Timestamp)
	recs := make(chan *Record)

	go MovingLimitRead(l.store, l.compID, "i", fromCh, toCh, recs, errc)
	var toLWM Timestamp
	fromCh <- l.startAtLWM
	for {
		select {
		case <-l.stopCh:
			l.stopCh <- struct{}{}
			return
		case recLWM := <-l.lwmCh:
			t := <-l.tmpT // FIXME: remove
			if recLWM <= toLWM {
				log.Println("LWM not increased", t)
			} else {
				toLWM = recLWM
				log.Printf("PROCESSING as result of %s", t)
				toCh <- toLWM
			}
		case r := <-recs:
			log.Println("linearizer forwarding", r)
			err := l.ltp.ProcessRecordLinearized(r)
			if err != nil {
				// fromCh <- r.Timestamp
				errc <- err
				return
			}
		}
	}
}
