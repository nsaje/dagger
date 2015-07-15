package structs

import "testing"
import "github.com/stretchr/testify/assert"

type testCase struct {
	in   string
	want Computation
}

func TestComputationFromString(t *testing.T) {
	cases := []testCase{
		{"avg(foo.metric)", Computation{"avg", "foo.metric"}},
		{"avg(foo.metric, bar.metric)", Computation{"avg", "foo.metric, bar.metric"}},
		{"alarm(foo.metric and bar.metric)", Computation{"alarm", "foo.metric and bar.metric"}},
	}
	for _, c := range cases {
		got, err := ComputationFromString(c.in)
		assert.Nil(t, err)
		assert.Equal(t, got.Name, c.want.Name)
		assert.Equal(t, got.Definition, c.want.Definition)
	}

	negative := []string{
		"avg foo.metric",
		"avg foo.metric)",
		"avg(foo.metric",
		"(foo.metric)",
	}
	for _, in := range negative {
		got, err := ComputationFromString(in)
		assert.Nil(t, got)
		assert.NotNil(t, err)
	}
}
