package dagger

import "testing"

type buffer []*Record

func (b buffer) Insert(_ StreamID, bufID string, t *Record) error {
	return nil
}

func (p buffer) ReadBuffer(compID StreamID, bufID string,
	from Timestamp, to Timestamp, recCh chan<- *Record, errc chan<- error,
	readCompletedCh chan struct{}) {
}

func TestMovingLimitRead(t *testing.T) {

}
