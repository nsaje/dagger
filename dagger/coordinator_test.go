package dagger

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Process struct {
	Cmd        *exec.Cmd
	StdoutPipe io.ReadCloser
	StdinPipe  io.WriteCloser
	StderrPipe io.ReadCloser
	t          *testing.T
}

func startProcess(t *testing.T, command string, args ...string) *Process {
	var err error
	var errPipe, outPipe io.ReadCloser
	var inPipe io.WriteCloser

	os.Setenv("DAGGER_PLUGIN_PATH", "../bin/")
	cmd := exec.Command(command, args...)
	p := &Process{Cmd: cmd, t: t}
	errPipe, err = cmd.StderrPipe()
	p.StderrPipe = errPipe
	if err != nil {
		goto Error
	}
	outPipe, err = cmd.StdoutPipe()
	p.StdoutPipe = outPipe
	if err != nil {
		goto Error
	}
	inPipe, err = cmd.StdinPipe()
	p.StdinPipe = inPipe
	if err != nil {
		goto Error
	}
	err = cmd.Start()
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

	proc := startProcess(t, cmdString, args...)
	return proc
}

func startSubscriber(t *testing.T, topic string) *Process {
	return startProcess(t, "../bin/dagger", "subscriber", topic)
}

func startProducer(t *testing.T, topic string) *Process {
	return startProcess(t, "../bin/dagger",
		"producer", "producer-stdin", topic)
}

func startWorker(t *testing.T) *Process {
	return startProcess(t, "../bin/dagger", "worker")
}

func (p *Process) stop() {
	err := p.Cmd.Process.Kill()
	if err != nil {
		p.t.Error("error killing process: ", err)
	}
	p.t.Log("Stderr for process", p.Cmd.Args)
	p.t.Log(dumpReader(p.StderrPipe))
}

func dumpReader(r io.Reader) string {
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	return buf.String()
}

func TestSubscription(t *testing.T) {
	// run Consul
	consul := startConsul(t)
	time.Sleep(2 * time.Second)

	sub := startSubscriber(t, "teststdin")
	pub := startProducer(t, "teststdin")

	pub.StdinPipe.Write([]byte("test\n"))

	time.Sleep(2 * time.Second)

	sub.stop()
	pub.stop()
	consul.stop()

	s := dumpReader(sub.StdoutPipe)
	assert.Contains(t, s, "test", "subscriber should get data")
}

func TestJobAcquisition(t *testing.T) {
	// run Consul
	consul := startConsul(t)
	time.Sleep(2 * time.Second)

	sub := startSubscriber(t, "foo(teststdin)")
	worker := startWorker(t)
	pub := startProducer(t, "teststdin")

	time.Sleep(1 * time.Second)
	pub.StdinPipe.Write([]byte("test\n"))
	time.Sleep(1 * time.Second)

	sub.stop()
	worker.stop()
	pub.stop()
	consul.stop()

	s := dumpReader(sub.StdoutPipe)
	assert.Contains(t, s, "fooized: test", "subscriber should get data")
}
