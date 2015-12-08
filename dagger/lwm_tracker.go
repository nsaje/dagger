package dagger

import (
	"log"
	"sync"
)

var maxTime = Timestamp(1<<63 - 1)

type LWMTracker interface {
	RecordProcessor
	SentTracker
	BeforeDispatching([]*Record)
	GetCombinedLWM() Timestamp
	GetLocalLWM() Timestamp
	GetUpstreamLWM() Timestamp
}

type lwmTracker struct {
	upstream     map[StreamID]Timestamp
	inProcessing map[string]Timestamp
	sync.RWMutex
}

func NewLWMTracker() LWMTracker {
	return &lwmTracker{
		make(map[StreamID]Timestamp),
		make(map[string]Timestamp),
		sync.RWMutex{},
	}
}

func (lwmT *lwmTracker) BeforeDispatching(ts []*Record) {
	lwmT.Lock()
	defer lwmT.Unlock()
	for _, r := range ts {
		lwmT.inProcessing[r.ID] = r.Timestamp
		log.Println("setting lwm", r.ID, r.Timestamp)
	}
}

func (lwmT *lwmTracker) SentSuccessfuly(compID StreamID, t *Record) error {
	lwmT.Lock()
	defer lwmT.Unlock()
	delete(lwmT.inProcessing, t.ID)
	log.Println("deleting lwm", t.ID, t.Timestamp)
	return nil
}

func (lwmT *lwmTracker) GetUpstreamLWM() Timestamp {
	lwmT.RLock()
	defer lwmT.RUnlock()
	min := Timestamp(1<<63 - 1)
	for _, lwm := range lwmT.upstream {
		if lwm < min {
			min = lwm
		}
	}
	return min
}

func (lwmT *lwmTracker) GetLocalLWM() Timestamp {
	lwmT.RLock()
	defer lwmT.RUnlock()
	min := Timestamp(1<<63 - 1)
	for _, lwm := range lwmT.inProcessing {
		if lwm < min {
			min = lwm
		}
	}
	return min
}

func (lwmT *lwmTracker) GetCombinedLWM() Timestamp {
	min := lwmT.GetUpstreamLWM()
	min2 := lwmT.GetLocalLWM()
	if min < min2 {
		return min
	}
	return min2
}

func (lwmT *lwmTracker) ProcessRecord(t *Record) error {
	lwmT.Lock()
	defer lwmT.Unlock()
	if t.LWM > lwmT.upstream[t.StreamID] {
		lwmT.upstream[t.StreamID] = t.LWM
	}
	return nil
}
