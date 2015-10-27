package dagger

import (
	"log"
	"sync"
	"time"

	"github.com/nsaje/dagger/structs"
)

var maxTime = time.Unix(1<<63-62135596801, 999999999)

type LWMTracker interface {
	TupleProcessor
	SentTracker
	BeforeDispatching([]*structs.Tuple)
	GetCombinedLWM() time.Time
	GetLocalLWM() time.Time
	GetUpstreamLWM() time.Time
}

type lwmTracker struct {
	upstream     map[string]time.Time
	inProcessing map[string]time.Time
	sync.RWMutex
}

func NewLWMTracker() LWMTracker {
	return &lwmTracker{
		make(map[string]time.Time),
		make(map[string]time.Time),
		sync.RWMutex{},
	}
}

func (lwmT *lwmTracker) BeforeDispatching(ts []*structs.Tuple) {
	lwmT.Lock()
	defer lwmT.Unlock()
	for _, t := range ts {
		lwmT.inProcessing[t.ID] = t.Timestamp
		log.Println("setting lwm", t.ID, t.Timestamp)
	}
}

func (lwmT *lwmTracker) SentSuccessfuly(compID string, t *structs.Tuple) error {
	lwmT.Lock()
	defer lwmT.Unlock()
	delete(lwmT.inProcessing, t.ID)
	log.Println("deleting lwm", t.ID, t.Timestamp)
	return nil
}

func (lwmT *lwmTracker) GetUpstreamLWM() time.Time {
	lwmT.RLock()
	defer lwmT.RUnlock()
	min := maxTime
	for _, lwm := range lwmT.upstream {
		if lwm.Before(min) {
			min = lwm
		}
	}
	return min
}

func (lwmT *lwmTracker) GetLocalLWM() time.Time {
	lwmT.RLock()
	defer lwmT.RUnlock()
	min := maxTime
	for _, lwm := range lwmT.inProcessing {
		if lwm.Before(min) {
			min = lwm
		}
	}
	return min
}

func (lwmT *lwmTracker) GetCombinedLWM() time.Time {
	min := lwmT.GetUpstreamLWM()
	min2 := lwmT.GetLocalLWM()
	if min.Before(min2) {
		return min
	}
	return min2
}

func (lwmT *lwmTracker) ProcessTuple(t *structs.Tuple) error {
	lwmT.Lock()
	defer lwmT.Unlock()
	if t.LWM.After(lwmT.upstream[t.StreamID]) {
		lwmT.upstream[t.StreamID] = t.LWM
	}
	return nil
}
