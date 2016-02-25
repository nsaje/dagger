package dagger

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	httpClient *http.Client
	url        string
	session    string
	active     bool
}

func NewClient(url string) (*Client, error) {
	tr := &http.Transport{}
	httpClient := &http.Client{Transport: tr}

	resp, err := httpClient.Get(fmt.Sprintf("%s/register", url))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	session, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	client := &Client{
		httpClient: httpClient,
		url:        url,
		session:    string(session),
		active:     true,
	}

	go client.renewPeriodically()

	return client, nil
}

func (dc Client) buildURL(format string, extras ...interface{}) string {
	return fmt.Sprintf(dc.url+format, extras...)
}

func (dc Client) renewPeriodically() {
	for dc.active {
		time.Sleep(5 * time.Second)
		_, err := dc.httpClient.Post(dc.buildURL("/renew?s=%s", dc.session), "text/plain", nil)
		if err != nil {
			panic(err)
		}
	}
}

func (dc Client) Subscribe(stream string) chan string {
	ch := make(chan string)
	resp, err := dc.httpClient.Get(dc.buildURL("/listen?s=%s", stream))
	go func() {
		if err != nil {
			log.Fatalf("error: %s", err)
		}
		log.Println("CLIENT response returned")
		defer close(ch)
		r := bufio.NewReader(resp.Body)
		defer resp.Body.Close()
		var line string
		for ; err == nil; line, err = r.ReadString('\n') {
			log.Println("CLIENT gotten", line)
			ch <- line
		}
	}()
	return ch
}

func (dc Client) Publish(stream string, record string) error {
	resp, err := dc.httpClient.Post(dc.buildURL("/submit_raw?s=%s&session=%s", stream, dc.session), "text/plain", strings.NewReader(record))
	defer resp.Body.Close()
	if err != nil {
		log.Fatalf("error: %s", err)
	}
	return nil
}
