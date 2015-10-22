package dagger

import (
	"log"
	"sync"
	"time"

	"github.com/nsaje/dagger/structs"
)

type LwmTracker interface {
	TupleProcessor
	SentTracker
	BeforeDispatching([]*structs.Tuple)
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
		lwmT.inProcessing[t.ID] = t.LWM
		log.Println("setting lwm", t.ID, t.LWM)
	}
}

func (lwmT *lwmTracker) SentSuccessfuly(compID string, t *structs.Tuple) error {
	lwmT.Lock()
	defer lwmT.Unlock()
	delete(lwmT.inProcessing, t.ID)
	log.Println("deleting lwm", t.ID, t.LWM)
	return nil
}

func (lwmT *lwmTracker) GetUpstreamLWM() (time.Time, error) {
	lwmT.RLock()
	defer lwmT.RUnlock()
	min := time.Now().Add(1 << 62)
	for _, lwm := range lwmT.upstream {
		if lwm.Before(min) {
			min = lwm
		}
	}
	return min, nil
}

func (lwmT *lwmTracker) GetLocalLWM() (time.Time, error) {
	min, _ := lwmT.GetUpstreamLWM()
	lwmT.RLock()
	defer lwmT.RUnlock()
	for _, lwm := range lwmT.inProcessing {
		if lwm.Before(min) {
			min = lwm
		}
	}
	return min, nil
}

func (lwmT *lwmTracker) ProcessTuple(t *structs.Tuple) error {
	lwmT.Lock()
	defer lwmT.Unlock()
	if t.LWM.After(lwmT.upstream[t.StreamID]) {
		lwmT.upstream[t.StreamID] = t.LWM
	}
	return nil
}
