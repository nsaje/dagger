package dagger

import (
	"net"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	s := "cpu_util, user_perc by hostname, device in avg($1, $2)"
	topics, matchBy, streamID, err := parseMatchDefinition(s)
	assert.Equal(t, err, nil)
	assert.Equal(t, []StreamID{"cpu_util", "user_perc"}, topics)
	assert.Equal(t, []string{"hostname", "device"}, matchBy)
	assert.Equal(t, StreamID("avg($1, $2)"), streamID)
}

func TestMatchTaskRun(t *testing.T) {
	t.Log("here we are")
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	srv := NewTestServer(t)
	defer srv.Stop()
	conf := api.DefaultConfig()
	conf.Address = srv.HTTPAddr
	coord := NewConsulCoordinator(func(conf *ConsulConfig) {
		conf.Address = srv.HTTPAddr
	}).(*consulCoordinator)
	coord.Start(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}, make(chan error))
	receiver := NewMockInputManager(mockCtrl)

	// NewMatchTask(coord, receiver, StreamID("match(cpu_util{service=monitoring}, user_perc{service=monitoring} by hostname, device in alarm(avg($1, 5min) )")
	sid := StreamID("match(s1{t1=v1}, s2{t1=v1} by t2, t3 in alarm(avg(%s)<2 and avg(%s)>3))")
	_, definition, err := ParseComputationID(sid)
	assert.Nil(t, err)
	taskInfo, _ := NewMatchTask(coord, receiver, sid, definition)

	receiver.EXPECT().SubscribeTo(StreamID("alarm(avg(s1{t1=v1,t2=a,t3=1})<2 and avg(s2{t1=v1,t2=a,t3=1})>3)"), Timestamp(0), gomock.Any())
	receiver.EXPECT().SubscribeTo(StreamID("alarm(avg(s1{t1=v1,t2=b,t3=1})<2 and avg(s2{t1=v1,t2=b,t3=1})>3)"), Timestamp(0), gomock.Any())
	receiver.EXPECT().SubscribeTo(StreamID("alarm(avg(s1{t1=v1,t2=a,t3=2})<2 and avg(s2{t1=v1,t2=a,t3=2})>3)"), Timestamp(0), gomock.Any())

	go taskInfo.Task.Run(make(chan error))

	coord.RegisterAsPublisher(StreamID("s1{t1=v1,t2=a,t3=1,t4=x}"))
	coord.RegisterAsPublisher(StreamID("s2{t1=v1,t2=a,t3=1,t4=x}"))
	coord.RegisterAsPublisher(StreamID("s1{t1=v1,t2=b,t3=1,t4=y}"))
	coord.RegisterAsPublisher(StreamID("s2{t1=v1,t2=a,t3=2,t4=x}"))
	time.Sleep(2 * time.Second)
}
