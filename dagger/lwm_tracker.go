package dagger

import (
	"log"
	"sync"
	"time"

	"github.com/nsaje/dagger/structs"
)

var maxTime = time.Unix(1<<63-62135596801, 999999999)

type LwmTracker interface {
	TupleProcessor
	SentTracker
	BeforeDispatching([]*structs.Tuple)
	GetCombinedLWM() (time.Time, error)
	GetLocalLWM() (time.Time, error)
	GetUpstreamLWM() (time.Time, error)
}

type lwmTracker struct {
	upstream     map[string]time.Time
	inProcessing map[string]time.Time
	sync.RWMutex
}

func NewLWMTracker() *lwmTracker {
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

func (lwmT *lwmTracker) GetUpstreamLWM() (time.Time, error) {
	lwmT.RLock()
	defer lwmT.RUnlock()
	min := maxTime
	for _, lwm := range lwmT.upstream {
		if lwm.Before(min) {
			min = lwm
		}
	}
	return min, nil
}

func (lwmT *lwmTracker) GetLocalLWM() (time.Time, error) {
	lwmT.RLock()
	defer lwmT.RUnlock()
	min := maxTime
	for _, lwm := range lwmT.inProcessing {
		if lwm.Before(min) {
			min = lwm
		}
	}
	return min, nil
}

func (lwmT *lwmTracker) GetCombinedLWM() (time.Time, error) {
	min, _ := lwmT.GetUpstreamLWM()
	min2, _ := lwmT.GetLocalLWM()
	if min.Before(min2) {
		return min, nil
	}
	return min2, nil
}

func (lwmT *lwmTracker) ProcessTuple(t *structs.Tuple) error {
	lwmT.Lock()
	defer lwmT.Unlock()
	if t.LWM.After(lwmT.upstream[t.StreamID]) {
		lwmT.upstream[t.StreamID] = t.LWM
	}
	return nil
}
