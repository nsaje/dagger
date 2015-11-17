package consul

import (
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/testutil"
	"github.com/nsaje/dagger/s"
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

func newTestServer(t *testing.T) *testutil.TestServer {
	// Create a server
	srv := testutil.NewTestServerConfig(t, func(c *testutil.TestServerConfig) {
		if !testing.Verbose() {
			c.LogLevel = "err"
		}
	})
	return srv
}

func TestSetWatcher(t *testing.T) {
	prefix := "prefix/"
	srv := newTestServer(t)
	defer srv.Stop()
	conf := api.DefaultConfig()
	conf.Address = srv.HTTPAddr
	kv := newSimpleKV(t, conf)
	coord := NewCoordinator(conf).(*consulCoordinator)
	newc, errc := coord.watchSet(prefix, nil)
	add := []string{
		"test/1",
		"test/2",
		"test/3",
	}
	remove := []string{
		"test/1",
		"test/2",
	}
	expected := [][]string{
		{},
		add[:1],
		add[:2],
		add[:3],
		add[1:],
		add[2:],
	}
	go func() {
		for _, k := range add {
			kv.Put(prefix+k, nil)
		}
		for _, k := range remove {
			kv.Delete(prefix + k)
		}
	}()
	var actual [][]string
	timeout := time.NewTimer(2 * time.Second)
LOOP:
	for {
		select {
		case <-timeout.C:
			break LOOP
		case k := <-newc:
			actual = append(actual, k)
			if len(actual) == len(expected) {
				break LOOP
			}
		case err := <-errc:
			t.Fatalf(err.Error())
		}
	}
	assert.Equal(t, expected, actual)
}

func TestSetDiffWatcher(t *testing.T) {
	// Create a server
	srv := newTestServer(t)
	defer srv.Stop()

	prefix := "prefix/"
	conf := api.DefaultConfig()
	conf.Address = srv.HTTPAddr
	kv := newSimpleKV(t, conf)
	coord := NewCoordinator(conf).(*consulCoordinator)
	addc, droppedc, errc := coord.watchSetDiff(prefix, nil)
	add := []string{
		"test/1",
		"test/2",
		"test/3",
	}
	remove := []string{
		"test/1",
		"test/2",
	}
	go func() {
		for _, k := range add {
			kv.Put(prefix+k, nil)
		}
		for _, k := range remove {
			kv.Delete(prefix + k)
		}
	}()
	var addedActual, droppedActual []string
	timeout := time.NewTimer(5 * time.Second)

	for {
		select {
		case <-timeout.C:
			t.Fatalf("Timeout!")
		case k := <-addc:
			addedActual = append(addedActual, k)
		case k := <-droppedc:
			droppedActual = append(droppedActual, k)
		case err := <-errc:
			t.Fatalf(err.Error())
		}
		if len(addedActual) == len(add) &&
			len(droppedActual) == len(remove) {
			break
		}
	}
	assert.Equal(t, add, addedActual)
}

func TestTaskWatcher(t *testing.T) {
	// Create a server
	srv := newTestServer(t)
	defer srv.Stop()

	conf := api.DefaultConfig()
	conf.Address = srv.HTTPAddr
	kv := newSimpleKV(t, conf)
	coord := NewCoordinator(conf)
	newc, errc := coord.WatchTasks(nil)
	add := []s.StreamID{
		"task1",
		"task2",
		"task3",
	}
	remove := []s.StreamID{
		"task1",
		"task2",
	}
	expected := [][]s.StreamID{
		{},
		add[:1],
		add[:2],
		add[:3],
		add[1:],
		add[2:],
	}
	go func() {
		for _, k := range add {
			kv.Put(taskPrefix+string(k), nil)
		}
		for _, k := range remove {
			kv.Delete(taskPrefix + string(k))
		}
	}()
	var actual [][]s.StreamID
	timeout := time.NewTimer(2 * time.Second)

	for {
		select {
		case <-timeout.C:
			t.Fatalf("Timeout!")
		case k := <-newc:
			t.Log("new set:", k)
			ks := make([]s.StreamID, len(k), len(k))
			for i := range k {
				ks[i] = s.StreamID(k[i])
			}
			actual = append(actual, ks)
			if len(actual) == len(expected) {
				assert.Equal(t, expected, actual)
				return
			}
		case err := <-errc:
			t.Fatalf(err.Error())
		}
	}
}

func TestWatchSubscribers(t *testing.T) {
	// Create a server
	srv := newTestServer(t)
	defer srv.Stop()

	prefix := subscribersPrefix
	conf := api.DefaultConfig()
	conf.Address = srv.HTTPAddr
	kv := newSimpleKV(t, conf)
	coord := NewCoordinator(conf).(*consulCoordinator)
	add := []string{
		"test/a",
		"test/b",
		"test{t1=v1,t2=v2}/c",
		"test{t1=v1}/d",
	}
	remove := []string{}
	addc, droppedc, errc := coord.WatchSubscribers(s.StreamID("test{t1=v1}"), nil)
	go func() {
		for _, k := range add {
			kv.Put(prefix+k, nil)
		}
		for _, k := range remove {
			kv.Delete(prefix + k)
		}
	}()
	var addedActual, droppedActual []string
	timeout := time.NewTimer(5 * time.Second)

	expectedA := []string{"a", "b", "d"}
	for {
		select {
		case <-timeout.C:
			t.Fatalf("Timeout!")
		case k := <-addc:
			addedActual = append(addedActual, k)
		case k := <-droppedc:
			droppedActual = append(droppedActual, k)
		case err := <-errc:
			t.Fatalf(err.Error())
		}
		if len(addedActual) == len(expectedA) &&
			len(droppedActual) == len(remove) {
			break
		}
	}
	assert.Equal(t, expectedA, addedActual)
}
