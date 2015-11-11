package consul

import (
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/testutil"
	"github.com/nsaje/dagger/dagger"
	"github.com/stretchr/testify/assert"
)

type simpleKV struct {
	KV *api.KV
	t  *testing.T
}

func (kv *simpleKV) Put(key string, value []byte) {
	pair := &api.KVPair{Key: key, Value: value}
	_, err := kv.KV.Put(pair, nil)
	if err != nil {
		kv.t.Fatalf(err.Error())
	}
}

func (kv *simpleKV) Delete(key string) {
	_, err := kv.KV.Delete(key, nil)
	if err != nil {
		kv.t.Fatalf(err.Error())
	}
}

func newSimpleKV(t *testing.T, conf *api.Config) *simpleKV {
	client, err := api.NewClient(conf)
	if err != nil {
		panic(err)
	}
	return &simpleKV{client.KV(), t}
}

func TestSetWatcher(t *testing.T) {
	// Create a server
	srv := testutil.NewTestServer(t)
	defer srv.Stop()

	conf := api.DefaultConfig()
	conf.Address = srv.HTTPAddr
	kv := newSimpleKV(t, conf)
	coord := NewCoordinator(conf)
	w := coord.newSetWatcher("test/")
	add := []string{
		"test/1",
		"test/2",
		"test/3",
	}
	remove := []string{
		"test/1",
		"test/2",
	}
	done := make(chan struct{})
	go func() {
		for _, k := range add {
			kv.Put(k, nil)
		}
		for _, k := range remove {
			kv.Delete(k)
		}
		close(done)
	}()
	var addedActual, droppedActual []string
	timeout := time.NewTimer(5 * time.Second)
	for {
		select {
		case <-done:
			return
		case <-timeout.C:
			t.Fatalf("Timeout!")
		case k := <-w.New():
			addedActual = append(addedActual, k)
			if len(addedActual) == len(add) {
				assert.Equal(t, add, addedActual)
			}
		case k := <-w.Dropped():
			droppedActual = append(droppedActual, k)
			if len(droppedActual) == len(remove) {
				assert.Equal(t, remove, droppedActual)
			}
		case err := <-w.Error():
			t.Fatalf(err.Error())
		}
	}
}

func TestTaskWatcher(t *testing.T) {
	// Create a server
	srv := testutil.NewTestServer(t)
	defer srv.Stop()

	conf := api.DefaultConfig()
	conf.Address = srv.HTTPAddr
	kv := newSimpleKV(t, conf)
	coord := NewCoordinator(conf)
	w := coord.NewTaskWatcher()
	add := []dagger.Task{
		"task1",
		"task2",
		"task3",
	}
	remove := []dagger.Task{
		"task1",
		"task2",
	}
	done := make(chan struct{})
	go func() {
		for _, k := range add {
			kv.Put(taskPrefix+string(k), nil)
		}
		for _, k := range remove {
			kv.Delete(taskPrefix + string(k))
		}
		close(done)
	}()
	var addedActual, droppedActual []dagger.Task
	timeout := time.NewTimer(5 * time.Second)
	for {
		select {
		case <-done:
			return
		case <-timeout.C:
			t.Fatalf("Timeout!")
		case k := <-w.New():
			addedActual = append(addedActual, k)
			if len(addedActual) == len(add) {
				assert.Equal(t, add, addedActual)
			}
		case k := <-w.Dropped():
			droppedActual = append(droppedActual, k)
			if len(droppedActual) == len(remove) {
				assert.Equal(t, remove, droppedActual)
			}
		case err := <-w.Error():
			t.Fatalf(err.Error())
		}
	}
}
