package dagger

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nsaje/dagger/structs"
	"github.com/twinj/uuid"
)

type HttpAPI struct {
	dispatcher  *Dispatcher
	subscribers httpSubscribers
}

func NewHttpAPI(receiver *Receiver, dispatcher *Dispatcher) HttpAPI {
	return HttpAPI{
		dispatcher,
		httpSubscribers{
			receiver: receiver,
			subs:     make(map[string][]chan *structs.Tuple),
			lock:     &sync.RWMutex{},
		},
	}
}

func (api HttpAPI) Serve() {
	http.HandleFunc("/submit", api.submitTuple)
	http.HandleFunc("/listen", api.listen)

	log.Fatal(http.ListenAndServe(":46632", nil))
}

func (api HttpAPI) submitTuple(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var t structs.Tuple
	err := decoder.Decode(&t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing JSON: %s", err), 400)
	}

	err = validate(&t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Tuple validation error: %s", err), 400)
	}

	t.LWM = time.Now()
	t.ID = uuid.NewV4().String()
	err = api.dispatcher.ProcessTuple(&t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error dispatching tuple: %s", err), 500)
	}
}

func validate(t *structs.Tuple) error {
	if len(t.StreamID) == 0 {
		return errors.New("'stream_id' should not be empty")
	}
	if strings.ContainsAny(t.StreamID, "()") {
		return errors.New("'stream_id' should not contain parentheses")
	}

	if t.Timestamp.IsZero() {
		return errors.New("'timestamp' is mandatory")
	}

	return nil
}

func (api HttpAPI) listen(w http.ResponseWriter, r *http.Request) {
	topicGlob := r.FormValue("s")
	if len(topicGlob) == 0 {
		http.Error(w, "stream id missing in URL", 400)
	}
	ch := make(chan *structs.Tuple)
	disconnected := w.(http.CloseNotifier).CloseNotify()

	w.Write([]byte("Subscribing to" + topicGlob + "\n"))
	w.(http.Flusher).Flush()

	api.subscribers.SubscribeTo(topicGlob, ch)
	for {
		select {
		case t := <-ch:
			json, _ := json.Marshal(t) // FIXME: error?
			w.Write(json)
			w.Write([]byte("\n"))
			w.(http.Flusher).Flush()
		case <-disconnected:
			api.subscribers.UnsubscribeFrom(topicGlob, ch)
			return
		}
	}
}

type httpSubscribers struct {
	receiver *Receiver
	subs     map[string][]chan *structs.Tuple
	lock     *sync.RWMutex
}

func (hs httpSubscribers) SubscribeTo(streamID string, ch chan *structs.Tuple) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	hs.subs[streamID] = append(hs.subs[streamID], ch)
	hs.receiver.SubscribeTo(streamID, hs)
}

func (hs httpSubscribers) UnsubscribeFrom(streamID string, ch chan *structs.Tuple) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	hs.receiver.UnsubscribeFrom(streamID, hs)
	idx := -1
	for i, c := range hs.subs[streamID] {
		if c == ch {
			idx = i
		}
	}
	if idx != -1 {
		hs.subs[streamID] = append(hs.subs[streamID][:idx], hs.subs[streamID][idx+1:]...)
	}
}

func (hs httpSubscribers) ProcessTuple(t *structs.Tuple) error {
	hs.lock.RLock()
	defer hs.lock.RUnlock()
	for _, ch := range hs.subs[t.StreamID] {
		ch <- t
	}
	return nil
}
