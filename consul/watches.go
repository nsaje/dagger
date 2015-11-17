package consul

import (
	"log"

	"github.com/hashicorp/consul/api"
)

type consulFunc func(*api.KV, string, uint64) (interface{}, *api.QueryMeta, error)

func (c *consulCoordinator) watch(key string, cf consulFunc, stop chan struct{}) (chan interface{}, chan error) {
	new := make(chan interface{})
	errc := make(chan error)
	go func() {
		kv := c.client.KV()
		var lastIndex uint64
		for {
			select {
			case <-stop:
				return
			default:
				// do a blocking query
				ret, queryMeta, err := cf(kv, key, lastIndex)
				log.Println("[consul] value updated at", key, ret)
				if err != nil {
					log.Println(err)
					errc <- err
					return
				}
				if ret != nil {
					new <- ret
				}
				lastIndex = queryMeta.LastIndex
			}
		}
	}()
	return new, errc
}

func consulGet(kv *api.KV, key string, lastIndex uint64) (interface{}, *api.QueryMeta, error) {
	return kv.Get(key, &api.QueryOptions{WaitIndex: lastIndex})
}

func consulKeys(kv *api.KV, prefix string, lastIndex uint64) (interface{}, *api.QueryMeta, error) {
	return kv.Keys(prefix, "", &api.QueryOptions{WaitIndex: lastIndex})
}

func (c *consulCoordinator) watchValue(key string, stop chan struct{}) (chan []byte, chan error) {
	new, errc := c.watch(key, consulGet, stop)
	vals := make(chan []byte)
	go func() {
		for {
			select {
			case <-stop:
				return
			case s := <-new:
				kvpair, ok := s.(*api.KVPair)
				if ok && kvpair != nil {
					vals <- kvpair.Value
				}
			}
		}
	}()
	return vals, errc
}

func (c *consulCoordinator) watchSet(prefix string, stop chan struct{}) (chan []string, chan error) {
	new, errc := c.watch(prefix, consulKeys, stop)
	sets := make(chan []string)
	go func() {
		for {
			select {
			case <-stop:
				return
			case s := <-new:
				keys := s.([]string)
				stripped := make([]string, len(keys), len(keys))
				for i := range keys {
					stripped[i] = keys[i][len(prefix):]
				}
				sets <- stripped
			}
		}
	}()
	return sets, errc
}

func (c *consulCoordinator) watchSetDiff(prefix string, stop chan struct{}) (chan string, chan string, chan error) {
	new, errc := c.watchSet(prefix, stop)
	added := make(chan string)
	dropped := make(chan string)
	go func() {
		var oldKeys []string
		for {
			select {
			case <-stop:
				return
			case newSet := <-new:
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
						added <- key
					}
					oldKeys = append(oldKeys, key)
				}
				for s := range diffSet {
					dropped <- s
				}
			}
		}
	}()
	return added, dropped, errc
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
