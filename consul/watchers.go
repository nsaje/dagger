package consul

import (
	"log"

	"github.com/hashicorp/consul/api"
)

// setWatcher watches for and notifies of changes on a KV prefix
type setWatcher struct {
	new  chan []string
	errc chan error
	done chan struct{}
}

func (c *consulCoordinator) newSetWatcher(prefix string) *setWatcher {
	w := &setWatcher{
		new:  make(chan []string),
		errc: make(chan error),
		done: make(chan struct{}),
	}
	go w.watch(c.client.KV(), prefix)
	return w
}

func (w *setWatcher) watch(kv *api.KV, prefix string) {
	var lastIndex uint64
	for {
		select {
		case <-w.done:
			return
		default:
			// do a blocking query
			keys, queryMeta, err := kv.Keys(prefix, "",
				&api.QueryOptions{WaitIndex: lastIndex})
			log.Println("[consul] keys updated in ", prefix, keys)
			if err != nil {
				log.Println(err)
				w.errc <- err
				return
			}
			if keys != nil {
				strippedKeys := make([]string, len(keys), len(keys))
				for i := range keys {
					strippedKeys[i] = keys[i][len(prefix):]
				}
				w.new <- strippedKeys
			}
			lastIndex = queryMeta.LastIndex
		}
	}
}

func (w *setWatcher) New() chan []string {
	return w.new
}

func (w *setWatcher) Error() chan error {
	return w.errc
}

func (w *setWatcher) Stop() {
	close(w.done)
}

type setDiffWatcher struct {
	*setWatcher
	added   chan string
	dropped chan string
}

func (c *consulCoordinator) newSetDiffWatcher(prefix string) *setDiffWatcher {
	w := &setDiffWatcher{
		setWatcher: c.newSetWatcher(prefix),
		added:      make(chan string),
		dropped:    make(chan string),
	}
	go w.watch()
	return w
}

func (w *setDiffWatcher) watch() {
	var oldKeys []string
	for {
		select {
		case <-w.done:
			return
		case newSet := <-w.setWatcher.New():
			// prepare a diff set so we can know which keys are newly
			// added and which are dropped
			diffSet := make(map[string]struct{})
			for _, s := range oldKeys {
				diffSet[s] = struct{}{}
			}
			oldKeys = make([]string, 0)
			for _, key := range newSet {
				_, exists := diffSet[key]
				if exists {
					delete(diffSet, key)
				} else {
					log.Println("[consul] new key ", key)
					w.added <- key
				}
				oldKeys = append(oldKeys, key)
			}
			for s := range diffSet {
				w.dropped <- s
			}
		}
	}
}

func (w *setDiffWatcher) Added() chan string {
	return w.added
}

func (w *setDiffWatcher) Dropped() chan string {
	return w.dropped
}

// func (w *SetWatcher) watch(prefix string) {
// 	lastIndex := uint64(0)
// 	kv := c.client.KV()
// 	oldVal := ""
// 	go func() {
// 		for {
// 			select {
// 			case <-stopCh:
// 				return
// 			default:
// 				pair, queryMeta, err := kv.Get(key, &api.QueryOptions{WaitIndex: lastIndex})
// 				if err != nil {
// 					log.Println("[ERROR] consul watch", err) // FIXME
// 				}
// 				var newVal string
// 				if pair != nil {
// 					newVal = string(pair.Value)
// 				}
// 				log.Println("[watch] new val:", newVal)
// 				if newVal != oldVal {
// 					oldVal = newVal
// 					value <- newVal
// 				}
// 				lastIndex = queryMeta.LastIndex
// 			}
// 		}
// 	}()
// 	return value
// }
