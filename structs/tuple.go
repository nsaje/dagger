package structs

import "time"

// Tuple is the atomic unit of data flowing through Dagger
type Tuple struct {
	Timestamp time.Time
	Data      interface{}
}
