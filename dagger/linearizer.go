package dagger

import (
	"container/heap"
	"fmt"
	"time"

	"github.com/nsaje/dagger/structs"
)

type linearizer struct {
	next        TupleProcessor
	input       chan submission
	upstreamLWM map[string]time.Time
}

func NewLinearizer(next TupleProcessor, streams []string) *linearizer {
	l := linearizer{
		next:        next,
		input:       make(chan submission),
		upstreamLWM: make(map[string]time.Time),
	}
	for _, s := range streams {
		l.upstreamLWM[s] = time.Time{}
	}
	return &l
}

type submission struct {
	t     *structs.Tuple
	errCh chan error
}

// min-heap of submissions
type submissionHeap []submission

func (h submissionHeap) Len() int           { return len(h) }
func (h submissionHeap) Less(i, j int) bool { return h[i].t.Timestamp.Before(h[j].t.Timestamp) }
func (h submissionHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *submissionHeap) Push(x interface{}) {
	*h = append(*h, x.(submission))
}

func (h *submissionHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (l *linearizer) ProcessTuple(t *structs.Tuple) error {
	subm := submission{t, make(chan error)}
	l.input <- subm
	// err := <-subm.errCh
	// return err
	return nil
}

func (l *linearizer) Linearize() {
	submHeap := &submissionHeap{}
	heap.Init(submHeap)

	for subm := range l.input {
		// update upstream LWM
		if subm.t.LWM.After(l.upstreamLWM[subm.t.StreamID]) {
			l.upstreamLWM[subm.t.StreamID] = subm.t.LWM
		}

		// insert into heap
		heap.Push(submHeap, subm)

		// recalculate LWM
		min := time.Now().Add(1 << 62)
		for _, lwm := range l.upstreamLWM {
			if lwm.Before(min) {
				min = lwm
			}
		}

		// flush tuples that have timestamp < lwm, but we can't flush if we
		// haven't seen at least one tuple from each upstream stream
		if !min.IsZero() {
			if submHeap.Len() > 1 {
				fmt.Println("MORETHAN1")
				tups := make([]submission, 0, submHeap.Len())
				for submHeap.Len() > 0 {
					tups = append(tups, heap.Pop(submHeap).(submission))
				}
				for _, tup := range tups {
					fmt.Print(tup.t.Data)
					fmt.Print(" ")
					heap.Push(submHeap, tup)
				}
				fmt.Println()
			}
			for submHeap.Len() > 0 && (*submHeap)[0].t.Timestamp.Before(min) {
				fmt.Println("FLUSHING")
				subm := heap.Pop(submHeap).(submission)
				// subm.errCh <- l.next.ProcessTuple(subm.t)
				l.next.ProcessTuple(subm.t)
			}
		}
	}
}
