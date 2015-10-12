package dagger

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
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
	subscribers *httpSubscribers
}

func NewHttpAPI(receiver *Receiver, dispatcher *Dispatcher) HttpAPI {
	return HttpAPI{
		dispatcher,
		&httpSubscribers{
			receiver: receiver,
			subs:     make(map[string]map[chan *structs.Tuple]struct{}),
			lock:     &sync.RWMutex{},
		},
	}
}

func (api HttpAPI) Serve() {
	http.HandleFunc("/submit", api.submit)
	http.HandleFunc("/submit_raw", api.submitRaw)
	http.HandleFunc("/listen", api.listen)

	log.Fatal(http.ListenAndServe(":46632", nil))
}

func (api HttpAPI) submit(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var t structs.Tuple
	err := decoder.Decode(&t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing JSON: %s", err), 400)
		return
	}

	err = validate(&t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Tuple validation error: %s", err), 400)
		return
	}

	t.LWM = time.Now() // FIXME: think this through
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

func (api HttpAPI) submitRaw(w http.ResponseWriter, r *http.Request) {
	streamID := r.FormValue("s")
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading POST body: %s", err), 500)
		return
	}
	t := structs.Tuple{
		StreamID:  streamID,
		ID:        uuid.NewV4().String(),
		Timestamp: time.Now(),
		LWM:       time.Now(),
		Data:      string(data),
	}
	err = api.dispatcher.ProcessTuple(&t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error dispatching tuple: %s", err), 500)
	}
}

func (api HttpAPI) listen(w http.ResponseWriter, r *http.Request) {
	topicGlob := r.FormValue("s")
	if len(topicGlob) == 0 {
		http.Error(w, "stream id missing in URL", 400)
		return
	}
	ch := make(chan *structs.Tuple)
	disconnected := w.(http.CloseNotifier).CloseNotify()

	api.subscribers.SubscribeTo(topicGlob, ch)
	defer api.subscribers.UnsubscribeFrom(topicGlob, ch)
	for {
		select {
		case t := <-ch:
			json, _ := json.Marshal(t) // FIXME: error?
			w.Write(json)
			w.Write([]byte("\n"))
			w.(http.Flusher).Flush()
		case <-disconnected:
			return
		}
	}
}

type httpSubscribers struct {
	receiver *Receiver
	subs     map[string]map[chan *structs.Tuple]struct{}
	lock     *sync.RWMutex
}

func (hs *httpSubscribers) SubscribeTo(streamID string, ch chan *structs.Tuple) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	subscribersSet := hs.subs[streamID]
	if subscribersSet == nil {
		subscribersSet = make(map[chan *structs.Tuple]struct{})
		hs.receiver.SubscribeTo(streamID, hs)
	}
	subscribersSet[ch] = struct{}{}
	hs.subs[streamID] = subscribersSet
}

func (hs *httpSubscribers) UnsubscribeFrom(streamID string, ch chan *structs.Tuple) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	delete(hs.subs[streamID], ch)
	if len(hs.subs[streamID]) == 0 {
		hs.receiver.UnsubscribeFrom(streamID, hs)
	}
}

func (hs *httpSubscribers) ProcessTuple(t *structs.Tuple) error {
	hs.lock.RLock()
	defer hs.lock.RUnlock()
	for ch := range hs.subs[t.StreamID] {
		ch <- t
	}
	return nil
}
