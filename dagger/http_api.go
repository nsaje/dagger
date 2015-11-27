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

	"github.com/twinj/uuid"
)

type HttpAPI struct {
	dispatcher  *Dispatcher
	subscribers *httpSubscribers
}

func NewHttpAPI(receiver Receiver, dispatcher *Dispatcher) HttpAPI {
	return HttpAPI{
		dispatcher,
		&httpSubscribers{
			receiver: receiver,
			subs:     make(map[StreamID]map[chan *Record]struct{}),
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
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading POST body: %s", err), 500)
		return
	}
	t, err := CreateRecordFromJSON(body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing record: %s", err), 500)
	}
	err = api.dispatcher.ProcessRecord(t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error dispatching record: %s", err), 500)
	}
}

func (api HttpAPI) submitRaw(w http.ResponseWriter, r *http.Request) {
	streamID := StreamID(r.FormValue("s"))
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading POST body: %s", err), 500)
		return
	}
	t, err := CreateRecord(streamID, string(data))
	if err != nil {
		http.Error(w, fmt.Sprintf("Error creating record: %s", err), 500)
	}
	err = api.dispatcher.ProcessRecord(t)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error dispatching record: %s", err), 500)
	}
}

// CreateRecordFromJSON parses a complete record from JSON and adds LWM and ID
func CreateRecordFromJSON(b []byte) (*Record, error) {
	var t Record
	err := json.Unmarshal(b, &t)
	if err != nil {
		return nil, fmt.Errorf("Error parsing JSON: %s", err)
	}

	err = validate(&t)
	if err != nil {
		return nil, fmt.Errorf("Record validation error: %s", err)
	}

	t.LWM = Timestamp(time.Now().UnixNano()) // FIXME: think this through
	t.ID = uuid.NewV4().String()

	return &t, nil
}

// CreateRecord creates a new record with given stream ID and data
func CreateRecord(streamID StreamID, data string) (*Record, error) {
	if len(streamID) == 0 {
		return nil, errors.New("Stream ID shouldn't be empty")
	}
	now := Timestamp(time.Now().UnixNano())
	t := Record{
		StreamID:  streamID,
		ID:        uuid.NewV4().String(),
		Timestamp: now,
		LWM:       now,
		Data:      string(data),
	}
	return &t, nil
}

func validate(t *Record) error {
	if len(t.StreamID) == 0 {
		return errors.New("'stream_id' should not be empty")
	}
	if strings.ContainsAny(string(t.StreamID), "()") {
		return errors.New("'stream_id' should not contain parentheses")
	}

	if t.Timestamp == 0 {
		return errors.New("'timestamp' is mandatory")
	}

	return nil
}

func (api HttpAPI) listen(w http.ResponseWriter, r *http.Request) {
	topicGlob := StreamID(r.FormValue("s"))
	if len(topicGlob) == 0 {
		http.Error(w, "stream id missing in URL", 400)
		return
	}
	ch := make(chan *Record)
	disconnected := w.(http.CloseNotifier).CloseNotify()

	api.subscribers.SubscribeTo(topicGlob, ch)
	defer api.subscribers.UnsubscribeFrom(topicGlob, ch)
	for {
		select {
		case r := <-ch:
			json, _ := json.Marshal(r) // FIXME: error?
			w.Write(json)
			w.Write([]byte("\n"))
			w.(http.Flusher).Flush()
		case <-disconnected:
			return
		}
	}
}

type httpSubscribers struct {
	receiver Receiver
	subs     map[StreamID]map[chan *Record]struct{}
	lock     *sync.RWMutex
}

func (hs *httpSubscribers) SubscribeTo(streamID StreamID, ch chan *Record) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	subscribersSet := hs.subs[streamID]
	if subscribersSet == nil {
		subscribersSet = make(map[chan *Record]struct{})
		hs.receiver.SubscribeTo(streamID, Timestamp(0), hs)
	}
	subscribersSet[ch] = struct{}{}
	hs.subs[streamID] = subscribersSet
}

func (hs *httpSubscribers) UnsubscribeFrom(streamID StreamID, ch chan *Record) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	delete(hs.subs[streamID], ch)
	if len(hs.subs[streamID]) == 0 {
		hs.receiver.UnsubscribeFrom(streamID, hs)
	}
}

func (hs *httpSubscribers) ProcessRecord(t *Record) error {
	hs.lock.RLock()
	defer hs.lock.RUnlock()
	for ch := range hs.subs[t.StreamID] {
		ch <- t
	}
	return nil
}
