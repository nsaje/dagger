package dagger

import "github.com/nsaje/dagger/s"

// TupleProcessor is an object capable of processing a tuple
type TupleProcessor interface {
	ProcessTuple(*s.Tuple) error
}

// LinearizedTupleProcessor is an object capable of processing a list of ordered
// tuples
type LinearizedTupleProcessor interface {
	ProcessTupleLinearized(*s.Tuple) error
}

// ProcessMultipleTuples processes multiple tuples with a single tuple processor
func ProcessMultipleTuples(tp TupleProcessor, tuples []*s.Tuple) error {
	errCh := make(chan error)
	for _, t := range tuples {
		go func(t *s.Tuple) {
			errCh <- tp.ProcessTuple(t)
		}(t)
	}

	// Return an error if any of the calls fail
	for i := 0; i < len(tuples); i++ {
		err := <-errCh
		if err != nil {
			return err
		}
	}
	return nil
}

// ProcessMultipleProcessors processes a single tuple with multiple tuple processors
func ProcessMultipleProcessors(procs []TupleProcessor, t *s.Tuple) error {
	errCh := make(chan error)
	for _, proc := range procs {
		go func(proc TupleProcessor) {
			errCh <- proc.ProcessTuple(t)
		}(proc)
	}

	// Return an error if any of the calls fail
	for i := 0; i < len(procs); i++ {
		err := <-errCh
		if err != nil {
			return err
		}
	}
	return nil
}
