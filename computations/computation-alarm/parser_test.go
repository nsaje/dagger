package main

import (
	"testing"

	"github.com/nsaje/dagger/dagger"
	"github.com/stretchr/testify/assert"
)

func TestParser(t *testing.T) {
	inputs := []string{
		"test > 15 times 3",
		"avg(cpu_util{t1=v1,t2=v2}, 5s) > 30.0 or avg(sum(test)) > 16.0 and sum(test2) < 13",
		"(avg(cpu_util{t1=v1,t2=v2}, 5s) > 30.0 or avg(sum(test)) > 16.0) and sum(test2) < 13",
		"avg(cpu_util{t1=v1,t2=v2}, 5s) > 30.0 or avg(sum(test{t1=v1})) > 16.0 and sum(test2) < 13, t1",
	}
	expected := []alarmDefinition{
		alarmDefinition{
			LeafNode{dagger.StreamID("test"), GT, 15.0, 3},
			"",
		},
		alarmDefinition{
			BinNode{OR,
				LeafNode{dagger.StreamID("avg(cpu_util{t1=v1,t2=v2}, 5s)"), GT, 30.0, 1},
				BinNode{AND,
					LeafNode{dagger.StreamID("avg(sum(test))"), GT, 16.0, 1},
					LeafNode{dagger.StreamID("sum(test2)"), LT, 13.0, 1}},
			},
			"",
		},
		alarmDefinition{
			BinNode{AND,
				BinNode{OR,
					LeafNode{dagger.StreamID("avg(cpu_util{t1=v1,t2=v2}, 5s)"), GT, 30.0, 1},
					LeafNode{dagger.StreamID("avg(sum(test))"), GT, 16.0, 1},
				},
				LeafNode{dagger.StreamID("sum(test2)"), LT, 13.0, 1},
			},
			"",
		},
		alarmDefinition{
			BinNode{OR,
				LeafNode{dagger.StreamID("avg(cpu_util{t1=v1,t2=v2}, 5s)"), GT, 30.0, 1},
				BinNode{AND,
					LeafNode{dagger.StreamID("avg(sum(test{t1=v1}))"), GT, 16.0, 1},
					LeafNode{dagger.StreamID("sum(test2)"), LT, 13.0, 1}},
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
