package dagger

// RecordProcessor is an object capable of processing a record
type RecordProcessor interface {
	ProcessRecord(*Record) error
}

// LinearizedRecordProcessor is an object capable of processing a list of ordered
// records
type LinearizedRecordProcessor interface {
	ProcessRecordLinearized(*Record) error
}

// ProcessMultipleRecords processes multiple records with a single record processor
func ProcessMultipleRecords(tp RecordProcessor, records []*Record) error {
	errCh := make(chan error)
	for _, r := range records {
		go func(t *Record) {
			errCh <- tp.ProcessRecord(t)
		}(r)
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
func ProcessMultipleProcessors(procs []RecordProcessor, t *Record) error {
	errCh := make(chan error)
	for _, proc := range procs {
		go func(proc RecordProcessor) {
			errCh <- proc.ProcessRecord(t)
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
