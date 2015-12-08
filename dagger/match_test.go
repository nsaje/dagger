package dagger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	s := "cpu_util, user_perc by hostname, device in avg($1, $2)"
	topics, matchBy, streamID, err := parseMatchDefinition(s)
	assert.Equal(t, err, nil)
	assert.Equal(t, []StreamID{"cpu_util", "user_perc"}, topics)
	assert.Equal(t, []string{"hostname", "device"}, matchBy)
	assert.Equal(t, StreamID("avg($1, $2)"), streamID)
}
