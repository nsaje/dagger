package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser(t *testing.T) {
	inputs := []string{
		"test > 15",
		"avg(cpu_util{t1=v1,t2=v2}, 5s) > 30.0 or avg(sum(test)) > 16.0 and sum(test2) < 13",
		"(avg(cpu_util{t1=v1,t2=v2}, 5s) > 30.0 or avg(sum(test)) > 16.0) and sum(test2) < 13",
		"avg(cpu_util{t1=v1,t2=v2}, 5s) > 30.0 or avg(sum(test{t1=v1})) > 16.0 and sum(test2) < 13, t1",
	}
	expected := []AlarmDefinition{
		AlarmDefinition{
			LeafNode{"test", GT, 15.0, 1},
			"",
		},
		AlarmDefinition{
			BinNode{OR,
				LeafNode{"avg(cpu_util{t1=v1,t2=v2}, 5s)", GT, 30.0, 1},
				BinNode{AND,
					LeafNode{"avg(sum(test))", GT, 16.0, 1},
					LeafNode{"sum(test2)", LT, 13.0, 1}},
			},
			"",
		},
		AlarmDefinition{
			BinNode{AND,
				BinNode{OR,
					LeafNode{"avg(cpu_util{t1=v1,t2=v2}, 5s)", GT, 30.0, 1},
					LeafNode{"avg(sum(test))", GT, 16.0, 1},
				},
				LeafNode{"sum(test2)", LT, 13.0, 1},
			},
			"",
		},
		AlarmDefinition{
			BinNode{OR,
				LeafNode{"avg(cpu_util{t1=v1,t2=v2}, 5s)", GT, 30.0, 1},
				BinNode{AND,
					LeafNode{"avg(sum(test{t1=v1}))", GT, 16.0, 1},
					LeafNode{"sum(test2)", LT, 13.0, 1}},
			},
			"t1",
		},
	}

	for i, input := range inputs {
		tree, err := Parse("alarmDef", []byte(input))
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, expected[i], tree)
	}
}
