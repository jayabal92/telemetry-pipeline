package metadata

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// --- Fake etcd client implementation ---

type fakeClient struct {
	data map[string]string
}

func newFakeClient() *fakeClient {
	return &fakeClient{data: make(map[string]string)}
}

func (c *fakeClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{ID: 1}, nil
}

func (c *fakeClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	c.data[key] = val
	return &clientv3.PutResponse{}, nil
}

func (c *fakeClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	var kvs []*mvccpb.KeyValue
	for k, v := range c.data {
		if k == key || (len(opts) > 0 && len(k) >= len(key) && k[:len(key)] == key) {
			kv := &mvccpb.KeyValue{}
			kv.Key = []byte(k)
			kv.Value = []byte(v)
			kvs = append(kvs, kv)
		}
	}
	return &clientv3.GetResponse{Kvs: kvs}, nil
}

func (c *fakeClient) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch := make(chan *clientv3.LeaseKeepAliveResponse)
	close(ch)
	return ch, nil
}

// --- Tests ---

func TestRegisterAndListBrokers(t *testing.T) {
	c := newFakeClient()
	s := NewStore(c)

	if err := s.RegisterBroker(context.Background(), "b1", "addr1", 5); err != nil {
		t.Fatalf("RegisterBroker failed: %v", err)
	}

	brokers, err := s.ListBrokers(context.Background())
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}
	if brokers["b1"] != "addr1" {
		t.Errorf("expected broker addr1, got %v", brokers["b1"])
	}
}

func TestCreateAndGetTopic(t *testing.T) {
	c := newFakeClient()
	s := NewStore(c)

	td := TopicDesc{Name: "topic1", Partitions: 2, RF: 1}
	if err := s.CreateTopic(context.Background(), td); err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	got, err := s.GetTopic(context.Background(), "topic1")
	if err != nil {
		t.Fatalf("GetTopic failed: %v", err)
	}
	if !reflect.DeepEqual(td, *got) {
		b1, _ := json.Marshal(td)
		b2, _ := json.Marshal(got)
		t.Errorf("expected %s, got %s", b1, b2)
	}
}

func TestSetAndGetPartitionState(t *testing.T) {
	c := newFakeClient()
	s := NewStore(c)

	st := PartitionState{Topic: "t1", Partition: 0, Leader: "b1", ISR: []string{"b1"}, LeaderEpoch: time.Now().Unix()}
	if err := s.SetPartitionState(context.Background(), st); err != nil {
		t.Fatalf("SetPartitionState failed: %v", err)
	}

	got, err := s.GetPartitionState(context.Background(), "t1", 0)
	if err != nil {
		t.Fatalf("GetPartitionState failed: %v", err)
	}
	if got.Leader != "b1" {
		t.Errorf("expected leader b1, got %v", got.Leader)
	}
}

func TestAssignInitialLeaders(t *testing.T) {
	c := newFakeClient()
	s := NewStore(c)

	// pre-register brokers
	c.Put(context.Background(), brokerKey("b1"), "addr1")
	c.Put(context.Background(), brokerKey("b2"), "addr2")

	err := s.AssignInitialLeaders(context.Background(), "topicX", 2, 2)
	if err != nil {
		t.Fatalf("AssignInitialLeaders failed: %v", err)
	}

	ps, err := s.GetPartitionState(context.Background(), "topicX", 0)
	if err != nil {
		t.Fatalf("GetPartitionState failed: %v", err)
	}
	if len(ps.ISR) != 2 {
		t.Errorf("expected 2 ISRs, got %d", len(ps.ISR))
	}
}

func TestAssignInitialLeaders_NotEnoughBrokers(t *testing.T) {
	c := newFakeClient()
	s := NewStore(c)

	c.Put(context.Background(), brokerKey("b1"), "addr1") // only one broker

	err := s.AssignInitialLeaders(context.Background(), "t2", 1, 2)
	if err == nil {
		t.Fatalf("expected error for insufficient brokers")
	}
	if got := err.Error(); got != "not enough brokers for rf=2" {
		t.Errorf("unexpected error: %v", got)
	}
}

func TestCommitAndFetchOffsets(t *testing.T) {
	c := newFakeClient()
	s := NewStore(c)

	if err := s.CommitOffset(context.Background(), "group1", "topic1", 0, 123); err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}

	offs, err := s.FetchGroupOffsets(context.Background(), "group1", "topic1")
	if err != nil {
		t.Fatalf("FetchGroupOffsets failed: %v", err)
	}
	if len(offs) != 1 || offs[0].Offset != 123 {
		t.Errorf("expected offset 123, got %+v", offs)
	}
}
