package dagger

import "github.com/nsaje/dagger/s"

// TupleProcessor is an object capable of processing a record
type TupleProcessor interface {
	ProcessTuple(*s.Record) error
}

// LinearizedTupleProcessor is an object capable of processing a list of ordered
// records
type LinearizedTupleProcessor interface {
	ProcessTupleLinearized(*s.Record) error
}

// ProcessMultipleTuples processes multiple records with a single record processor
func ProcessMultipleTuples(tp TupleProcessor, records []*s.Record) error {
	errCh := make(chan error)
	for _, t := range records {
		go func(t *s.Record) {
			errCh <- tp.ProcessTuple(t)
		}(t)
	}

	// Return an error if any of the calls fail
	for i := 0; i < len(records); i++ {
		err := <-errCh
		if err != nil {
			return err
		}
	}
	return nil
}

// ProcessMultipleProcessors processes a single record with multiple record processors
func ProcessMultipleProcessors(procs []TupleProcessor, t *s.Record) error {
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
