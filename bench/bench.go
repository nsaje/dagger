/*
A derivative of https://github.com/nsqio/nsq/blob/master/bench/bench_writer/bench_writer.go

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsaje/dagger/client"
)

var (
	role     = flag.String("role", "publish", "publish/subscribe")
	runfor   = flag.Duration("runfor", 10*time.Second, "duration of time to run")
	url      = flag.String("url", "127.0.0.1:46666", "http://<addr>:<port> to connect to Dagger HTTP API")
	streamID = flag.String("streamID", "bench", "stream to publish messages to")
	size     = flag.Int("size", 8, "size of messages")
	deadline = flag.String("deadline", "", "deadline to start the benchmark run")
)

var totalMsgCount int64

func main() {
	var wg sync.WaitGroup
	flag.Parse()

	goChan := make(chan int)
	rdyChan := make(chan int)
	worker := subWorker
	if *role == "publish" {
		worker = pubWorker
	}
	for j := 0; j < runtime.GOMAXPROCS(0); j++ {
		wg.Add(1)
		go func() {
			worker(*runfor, *url, *streamID, rdyChan, goChan)
			wg.Done()
		}()
		<-rdyChan
	}

	if *deadline != "" {
		t, err := time.Parse("2006-01-02 15:04:05", *deadline)
		if err != nil {
			log.Fatal(err)
		}
		d := t.Sub(time.Now())
		log.Printf("sleeping until %s (%s)", t, d)
		time.Sleep(d)
	}

	start := time.Now()
	close(goChan)
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	tmc := atomic.LoadInt64(&totalMsgCount)
	log.Printf("duration: %s - %.03fops/s - %.03fus/op",
		duration,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/float64(tmc))
}

func pubWorker(runfor time.Duration, url string, streamID string, rdyChan chan int, goChan chan int) {
	client, err := dagger.NewClient(url)
	if err != nil {
		panic(err)
	}
	rdyChan <- 1
	<-goChan
	var msgCount int64
	endTime := time.Now().Add(runfor)
	var i int64
	for {
		err := client.Publish(streamID, fmt.Sprintf("a%d", i))
		i++
		if err != nil {
			panic(err)
		}
		msgCount++
		if time.Now().After(endTime) {
			break
		}
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
}

func subWorker(runfor time.Duration, url string, streamID string, rdyChan chan int, goChan chan int) {
	client, err := dagger.NewClient(url)
	if err != nil {
		panic(err)
	}
	ch := client.Subscribe(streamID)
	rdyChan <- 1
	<-goChan
	var msgCount int64
	endTime := time.Now().Add(runfor)
	for {
		_, ok := <-ch
		if !ok {
			break
		}
		msgCount++
		if time.Now().After(endTime) {
			break
		}
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
}
