package dagger

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var ConsulReadyText = "agent: Synced service"
var DaggerReadyText = "coordinator ready"

type Buffer struct {
	bytes.Buffer
	sync.RWMutex
}

func (b *Buffer) String() string {
	b.RLock()
	defer b.RUnlock()
	return b.Buffer.String()
}

type Process struct {
	Cmd          *exec.Cmd
	StdoutBuffer *Buffer
	StdinPipe    io.WriteCloser
	StderrBuffer *Buffer
	t            *testing.T
	errCh        chan error
}

func watchReady(p *Process, readyText string, r io.Reader, b *Buffer) {
	scanner := bufio.NewScanner(r)
	for {
		if !scanner.Scan() {
			break
		}
		text := scanner.Text()
		fmt.Println("read", text)
		if strings.Contains(strings.ToLower(text), "failed to start consul") {
			p.errCh <- fmt.Errorf("error occurred when starting %v", p.Cmd.Args)
		}
		if strings.Contains(text, readyText) {
			close(p.errCh)
		}
		b.Lock()
		b.WriteString(text)
		b.WriteRune('\n')
		b.Unlock()
	}
}

func startProcess(t *testing.T, readyText string, command string, args ...string) *Process {
	var err error
	var errPipe, outPipe io.ReadCloser
	var inPipe io.WriteCloser

	os.Setenv("DAGGER_PLUGIN_PATH", "../bin/")
	cmd := exec.Command(command, args...)
	p := &Process{
		Cmd:          cmd,
		t:            t,
		StdoutBuffer: &Buffer{},
		StderrBuffer: &Buffer{},
	}

	// watch output streams and copy them to buffers for later inspection
	p.errCh = make(chan error)
	errPipe, err = cmd.StderrPipe()
	if err != nil {
		goto Error
	}
	go watchReady(p, readyText, errPipe, p.StderrBuffer)
	outPipe, err = cmd.StdoutPipe()
	if err != nil {
		goto Error
	}
	go watchReady(p, readyText, outPipe, p.StdoutBuffer)

	// set up stdin pipe for submitting input to processes
	inPipe, err = cmd.StdinPipe()
	p.StdinPipe = inPipe
	if err != nil {
		goto Error
	}

	// start the process
	err = cmd.Start()
	if err != nil {
		goto Error
	}

	err = <-p.errCh
	if err != nil {
		goto Error
	}

	return p

Error:
	t.Error("error starting command", command, err)
	return nil
}

func startConsul(t *testing.T) *Process {
	// clear Consul dir
	consulDir := "/tmp/consul"
	os.RemoveAll(consulDir)

	cmdString := "consul"
	args := []string{"agent", "-server", "-bootstrap-expect", "1",
		"-data-dir", "/tmp/consul"}

	proc := startProcess(t, ConsulReadyText, cmdString, args...)
	return proc
}

func startSubscriber(t *testing.T, topic string) *Process {
	return startProcess(t, DaggerReadyText, "../bin/dagger", "subscriber", "-dataonly", topic)
}

func startProducer(t *testing.T, topic string) *Process {
	return startProcess(t, DaggerReadyText, "../bin/dagger",
		"producer", "producer-stdin", topic)
}

func startWorker(t *testing.T) *Process {
	return startProcess(t, DaggerReadyText, "../bin/dagger", "worker")
}

func (p *Process) stop() {
	if p != nil {
		err := p.Cmd.Process.Signal(os.Interrupt)
		if err != nil {
			p.t.Error("error killing process: ", err)
		}
		p.t.Log("Stderr for process", p.Cmd.Args)
		p.t.Log(p.StderrBuffer.String())
	}
}

func dumpReader(r io.Reader) string {
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	return buf.String()
}

func TestSubscription(t *testing.T) {
	var consul, sub, pub *Process
	// run Consul
	consul = startConsul(t)
	defer consul.stop()

	sub = startSubscriber(t, "teststdin")
	defer sub.stop()
	pub = startProducer(t, "teststdin")
	defer pub.stop()

	pub.StdinPipe.Write([]byte("test\n"))

	time.Sleep(1 * time.Second)

	s := sub.StdoutBuffer.String()
	assert.Contains(t, s, "test", "subscriber should get data")
}

func TestJobAcquisition(t *testing.T) {
	// run Consul
	consul := startConsul(t)
	defer consul.stop()

	sub := startSubscriber(t, "foo(teststdin)")
	defer sub.stop()
	worker := startWorker(t)
	defer worker.stop()
	pub := startProducer(t, "teststdin")
	defer pub.stop()

	time.Sleep(1 * time.Second)
	pub.StdinPipe.Write([]byte("test\n"))
	time.Sleep(1 * time.Second)

	s := sub.StdoutBuffer.String()
	assert.Contains(t, s, "fooized: test", "subscriber should get data")
}

func TestFailover(t *testing.T) {
	// run Consul
	consul := startConsul(t)
	defer consul.stop()

	sub := startSubscriber(t, "foo(teststdin)")
	defer sub.stop()
	worker1 := startWorker(t)
	defer worker1.stop()
	pub := startProducer(t, "teststdin")
	defer pub.stop()

	input := []string{"1", "2", "3", "4", "5"}
	expected := bytes.Buffer{}
	for _, inp := range input {
		expected.WriteString("fooized: ")
		expected.WriteString(inp)
		expected.WriteString("\n")
	}
	time.Sleep(1 * time.Second)
	go func() {
		for _, inp := range input {
			time.Sleep(1 * time.Second)
			pub.StdinPipe.Write([]byte(inp + "\n"))
		}
	}()
	time.Sleep(1 * time.Second)
	worker2 := startWorker(t)
	defer worker2.stop()

	time.Sleep(1 * time.Second)
	worker1.stop()

	time.Sleep(6 * time.Second)

	actual := sub.StdoutBuffer.String()
	assert.Equal(t, expected.String(), actual, "subscriber should get data")
}
