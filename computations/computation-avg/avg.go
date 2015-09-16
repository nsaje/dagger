package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/nsaje/dagger/computations"
	"github.com/nsaje/dagger/structs"
	"github.com/twinj/uuid"
)

// AvgComputation calculates averages of numeric values over time periods
type AvgComputation struct {
	counter int
}

func (c *AvgComputation) GetInfo(definition string) (structs.ComputationPluginInfo, error) {
	var stream string
	var period string
	info := structs.ComputationPluginInfo{}
	info.Stateful = true

	_, err := fmt.Sscanf(definition, "%s, %s", &stream, &period)
	if err != nil {
		return info, err
	}
	info.Inputs = []string{stream, fmt.Sprintf("time(%s)", period)}
	return info, nil
}

func (c *AvgComputation) GetState() ([]byte, error) {
	return []byte(fmt.Sprintf("%d", c.counter)), nil
}

func (c *AvgComputation) SetState(state []byte) error {
	counter, err := strconv.Atoi(string(state))
	if err != nil {
		return fmt.Errorf("Error setting state %s in plugin: %s",
			string(state),
			err)
	}
	c.counter = counter
	return nil
}

func (c *AvgComputation) SubmitTuple(t *structs.Tuple) ([]*structs.Tuple, error) {
	t.Data = fmt.Sprintf("fooized: %v, state: %v", t.Data, c.counter)
	t.ID = uuid.NewV4().String()
	c.counter++
	return []*structs.Tuple{t}, nil
}

func main() {
	log.SetPrefix("[AvgComputation log] ")
	log.Printf("AvgComputation started")
	c := &AvgComputation{1}
	computations.StartPlugin(c)
}
