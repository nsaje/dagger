package dagger

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nsaje/dagger/structs"
	"github.com/stretchr/testify/assert"
)

type sink struct {
	tuples []*structs.Tuple
}

func (s *sink) ProcessTuple(t *structs.Tuple) error {
	s.tuples = append(s.tuples, t)
	return nil
}

func TestLinearizer(test *testing.T) {
	now := time.Now()
	n := 10

	// prepare tuples, but in random order
	perm := rand.Perm(n)
	in := make([]*structs.Tuple, n)
	inPermuted := make([]*structs.Tuple, n)
	for i := range in {
		t := &structs.Tuple{
			StreamID:  "testStream",
			ID:        strconv.Itoa(i),
			Timestamp: now.Add(time.Duration(i) * time.Second),
		}
		in[i] = t
		inPermuted[perm[i]] = t
	}

	next := &sink{make([]*structs.Tuple, 0)}
	linearizer := NewLinearizer(next, []string{"testStream"})
	go linearizer.Linearize()

	toSend := inPermuted
	wg := sync.WaitGroup{}
	for len(toSend) > 0 {
		minTimestamp := toSend[0].Timestamp
		for _, t := range toSend[1:] {
			if t.Timestamp.Before(minTimestamp) {
				minTimestamp = t.Timestamp
			}
		}
		if len(toSend) == 1 {
			// last tuple, make LWM larger so it flushes
			minTimestamp = minTimestamp.Add(time.Hour)
		}
		toSend[0].LWM = minTimestamp.Add(time.Millisecond)
		wg.Add(1)
		go func(t *structs.Tuple) {
			defer wg.Done()
			linearizer.ProcessTuple(t)
		}(toSend[0])
		toSend = toSend[1:]
		time.Sleep(time.Millisecond)
	}

	wg.Wait()

	var receivedIDs []int
	var expectedIDs []int
	for i, t := range next.tuples {
		if t == nil {
			continue
		}
		id, _ := strconv.Atoi(t.ID)
		receivedIDs = append(receivedIDs, id)
		expectedIDs = append(expectedIDs, i)
	}
	assert.EqualValues(test, expectedIDs, receivedIDs)
}
