package dagger

import (
	"log"
	"time"

	"github.com/nsaje/dagger/structs"
)

type LwmTracker interface {
	TupleProcessor
	SentTracker
	BeforeDispatching([]*structs.Tuple)
	GetLWM() (time.Time, error)
}

type lwmTracker struct {
	upstream     map[string]time.Time
	inProcessing map[string]time.Time
}

func NewLWMTracker() *lwmTracker {
	return &lwmTracker{
		make(map[string]time.Time),
		make(map[string]time.Time),
	}
}

func (lwmT *lwmTracker) BeforeDispatching(ts []*structs.Tuple) {
	for _, t := range ts {
		lwmT.inProcessing[t.ID] = t.LWM
	}
}

func (lwmT *lwmTracker) SentSuccessfuly(compID string, t *structs.Tuple) error {
	delete(lwmT.inProcessing, t.ID)
	return nil
}

func (lwmT *lwmTracker) GetLWM() (time.Time, error) {
	min := time.Now().Add(1 << 62)
	for _, lwm := range lwmT.inProcessing {
		if lwm.Before(min) {
			min = lwm
		}
	}
	for _, lwm := range lwmT.upstream {
		if lwm.Before(min) {
			min = lwm
		}
	}
	log.Println("[lwm_tracker]", min, lwmT.upstream, lwmT.inProcessing)
	return min, nil
}

func (lwmT *lwmTracker) ProcessTuple(t *structs.Tuple) error {
	lwmT.upstream[t.StreamID] = t.LWM
	return nil
}
