package dagger

import (
	"log"
	"sync"

	"github.com/nsaje/dagger/s"
)

var maxTime = s.Timestamp(1<<63 - 1)

type LWMTracker interface {
	RecordProcessor
	SentTracker
	BeforeDispatching([]*s.Record)
	GetCombinedLWM() s.Timestamp
	GetLocalLWM() s.Timestamp
	GetUpstreamLWM() s.Timestamp
}

type lwmTracker struct {
	upstream     map[s.StreamID]s.Timestamp
	inProcessing map[string]s.Timestamp
	sync.RWMutex
}

func NewLWMTracker() LWMTracker {
	return &lwmTracker{
		make(map[s.StreamID]s.Timestamp),
		make(map[string]s.Timestamp),
		sync.RWMutex{},
	}
}

func (lwmT *lwmTracker) BeforeDispatching(ts []*s.Record) {
	lwmT.Lock()
	defer lwmT.Unlock()
	for _, r := range ts {
		lwmT.inProcessing[r.ID] = r.Timestamp
		log.Println("setting lwm", r.ID, r.Timestamp)
	}
}

func (lwmT *lwmTracker) SentSuccessfuly(compID s.StreamID, t *s.Record) error {
	lwmT.Lock()
	defer lwmT.Unlock()
	delete(lwmT.inProcessing, t.ID)
	log.Println("deleting lwm", t.ID, t.Timestamp)
	return nil
}

func (lwmT *lwmTracker) GetUpstreamLWM() s.Timestamp {
	lwmT.RLock()
	defer lwmT.RUnlock()
	min := s.Timestamp(1<<63 - 1)
	for _, lwm := range lwmT.upstream {
		if lwm < min {
			min = lwm
		}
	}
	return min
}

func (lwmT *lwmTracker) GetLocalLWM() s.Timestamp {
	lwmT.RLock()
	defer lwmT.RUnlock()
	min := s.Timestamp(1<<63 - 1)
	for _, lwm := range lwmT.inProcessing {
		if lwm < min {
			min = lwm
		}
	}
	return min
}

func (lwmT *lwmTracker) GetCombinedLWM() s.Timestamp {
	min := lwmT.GetUpstreamLWM()
	min2 := lwmT.GetLocalLWM()
	if min < min2 {
		return min
	}
	return min2
}

func (lwmT *lwmTracker) ProcessRecord(t *s.Record) error {
	lwmT.Lock()
	defer lwmT.Unlock()
	if t.LWM > lwmT.upstream[t.StreamID] {
		lwmT.upstream[t.StreamID] = t.LWM
	}
	return nil
}
