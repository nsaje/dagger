package dagger

import "github.com/nsaje/dagger/structs"

// TupleProcessor is an object capable of processing a tuple
type TupleProcessor interface {
	ProcessTuple(*structs.Tuple) error
}

// ProcessMultipleTuples processes multiple tuples with a single tuple processor
func ProcessMultipleTuples(tp TupleProcessor, tuples []*structs.Tuple) error {
	errCh := make(chan error)
	for _, t := range tuples {
		go func(t *structs.Tuple) {
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
func ProcessMultipleProcessors(procs []TupleProcessor, t *structs.Tuple) error {
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
