package broker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"telemetry-pipeline/internal/config"
	"telemetry-pipeline/internal/metadata"
	"telemetry-pipeline/proto"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// --- fake metadata.Store for unit test ---
type fakeStore struct {
	*metadata.Store // embed real struct so type matches
	topics          map[string]metadata.TopicDesc
	brokers         map[string]string
	partMeta        *metadata.PartitionState
}

func (f *fakeStore) RegisterBroker(ctx context.Context, id, addr string, ttl int64) error {
	f.brokers[id] = addr
	return nil
}
func (f *fakeStore) ListBrokers(ctx context.Context) (map[string]string, error) {
	return f.brokers, nil
}
func (f *fakeStore) GetTopic(ctx context.Context, t string) (*metadata.TopicDesc, error) {
	if td, ok := f.topics[t]; ok {
		return &td, nil
	}
	return nil, context.Canceled
}
func (f *fakeStore) GetPartitionState(ctx context.Context, t string, p int) (*metadata.PartitionState, error) {
	return f.partMeta, nil
}
func (f *fakeStore) CreateTopic(ctx context.Context, td metadata.TopicDesc) error {
	f.topics[td.Name] = td
	return nil
}
func (f *fakeStore) AssignInitialLeaders(ctx context.Context, topic string, parts int, rf int) error {
	return nil
}
func (f *fakeStore) CommitOffset(ctx context.Context, group, topic string, part int, off int64) error {
	return nil
}
func (f *fakeStore) FetchGroupOffsets(ctx context.Context, group, topic string) ([]metadata.GroupPartitionOffset, error) {
	return nil, nil
}

// --- helper to spin up in-memory server ---
func startTestBroker(t *testing.T, dir string) (*Server, proto.MQClient, func()) {
	z := zap.NewNop()
	cfg := config.Config{
		Server: config.ServerConfig{
			NodeID:   "n1",
			GRPCAddr: "127.0.0.1:0",
			HTTPAddr: "127.0.0.1:0",
		},
		Storage: config.StorageConfig{
			DataDir:      dir,
			SegmentBytes: 1024,
		},
	}
	// fake etcd
	etcd := &clientv3.Client{} // unused in fake store
	s := NewServer(cfg, z, etcd)

	// override store with fake one
	s.store = &fakeStore{
		topics: map[string]metadata.TopicDesc{
			"events": {Name: "events", Partitions: 1, RF: 1},
		},
		brokers: map[string]string{"n1": "127.0.0.1:0"},
		partMeta: &metadata.PartitionState{
			Leader:      "n1",
			LeaderEpoch: 1,
			ISR:         []string{"n1"},
		},
	}

	// real GRPC server on random port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen err: %v", err)
	}
	s.GRPCLis = lis
	s.GRPCSrv = grpc.NewServer()
	proto.RegisterMQServer(s.GRPCSrv, s)

	go s.GRPCSrv.Serve(lis)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial err: %v", err)
	}
	client := proto.NewMQClient(conn)

	cleanup := func() {
		s.Shutdown(context.Background())
		_ = conn.Close()
	}
	return s, client, cleanup
}

func TestProduceAndFetch(t *testing.T) {
	dir := t.TempDir()
	_, client, cleanup := startTestBroker(t, dir)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// --- Produce ---
	msg := &proto.Message{Key: []byte("k1"), Value: []byte("v1")}
	resp, err := client.Produce(ctx, &proto.ProduceRequest{
		Topic:     "events",
		Partition: 0,
		Messages:  []*proto.Message{msg},
	})
	if err != nil {
		t.Fatalf("Produce error: %v", err)
	}
	if len(resp.Offsets) != 1 || resp.Offsets[0] != 0 {
		t.Fatalf("expected offset=0 got %v", resp.Offsets)
	}

	// --- Fetch ---
	fresp, err := client.Fetch(ctx, &proto.FetchRequest{
		Topic:       "events",
		Partition:   0,
		Offset:      0,
		MaxMessages: 10,
	})
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if len(fresp.Records) != 1 {
		t.Fatalf("expected 1 record got %d", len(fresp.Records))
	}
	got := fresp.Records[0].Message
	if string(got.Value) != "v1" {
		t.Fatalf("expected value v1 got %s", got.Value)
	}
}

func TestBatchProduceAndFetch(t *testing.T) {
	dir := t.TempDir()
	_, client, cleanup := startTestBroker(t, dir)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// --- Batch Produce ---
	msgs := []*proto.Message{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}
	resp, err := client.Produce(ctx, &proto.ProduceRequest{
		Topic:     "events",
		Partition: 0,
		Messages:  msgs,
	})
	if err != nil {
		t.Fatalf("Produce error: %v", err)
	}
	if len(resp.Offsets) != 3 {
		t.Fatalf("expected 3 offsets got %v", resp.Offsets)
	}
	for i, off := range resp.Offsets {
		if off != int64(i) {
			t.Fatalf("expected offset %d got %d", i, off)
		}
	}

	// --- Batch Fetch ---
	fresp, err := client.Fetch(ctx, &proto.FetchRequest{
		Topic:       "events",
		Partition:   0,
		Offset:      0,
		MaxMessages: 10,
	})
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if len(fresp.Records) != 3 {
		t.Fatalf("expected 3 records got %d", len(fresp.Records))
	}
	for i, rec := range fresp.Records {
		expVal := []byte(fmt.Sprintf("v%d", i+1))
		if string(rec.Message.Value) != string(expVal) {
			t.Errorf("expected %s got %s", expVal, rec.Message.Value)
		}
	}
}

func TestConcurrentProduce(t *testing.T) {
	dir := t.TempDir()
	_, client, cleanup := startTestBroker(t, dir)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const workers = 5
	const msgsPerWorker = 10
	total := workers * msgsPerWorker

	var wg sync.WaitGroup
	wg.Add(workers)

	// Concurrent produce
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < msgsPerWorker; i++ {
				msg := &proto.Message{
					Key:   []byte(fmt.Sprintf("k-%d-%d", id, i)),
					Value: []byte(fmt.Sprintf("v-%d-%d", id, i)),
				}
				_, err := client.Produce(ctx, &proto.ProduceRequest{
					Topic:     "events",
					Partition: 0,
					Messages:  []*proto.Message{msg},
				})
				if err != nil {
					t.Errorf("worker %d produce error: %v", id, err)
				}
			}
		}(w)
	}
	wg.Wait()

	// Fetch all messages
	fresp, err := client.Fetch(ctx, &proto.FetchRequest{
		Topic:       "events",
		Partition:   0,
		Offset:      0,
		MaxMessages: int32(total),
	})
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if len(fresp.Records) != total {
		t.Fatalf("expected %d records got %d", total, len(fresp.Records))
	}

	// Offsets must be sequential
	for i, rec := range fresp.Records {
		if rec.Offset != int64(i) {
			t.Errorf("expected offset %d got %d", i, rec.Offset)
		}
	}
}

func TestConcurrentProduceAndFetch(t *testing.T) {
	dir := t.TempDir()
	_, client, cleanup := startTestBroker(t, dir)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const workers = 3
	const msgsPerWorker = 5
	total := workers * msgsPerWorker

	var wg sync.WaitGroup
	wg.Add(workers)

	// Produce in background
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < msgsPerWorker; i++ {
				msg := &proto.Message{
					Key:   []byte(fmt.Sprintf("k-%d-%d", id, i)),
					Value: []byte(fmt.Sprintf("v-%d-%d", id, i)),
				}
				_, err := client.Produce(ctx, &proto.ProduceRequest{
					Topic:     "events",
					Partition: 0,
					Messages:  []*proto.Message{msg},
				})
				if err != nil {
					t.Errorf("worker %d produce error: %v", id, err)
				}
				time.Sleep(50 * time.Millisecond) // stagger a bit
			}
		}(w)
	}

	// Concurrent fetch while producing
	var fetchWg sync.WaitGroup
	fetchWg.Add(1)
	go func() {
		defer fetchWg.Done()
		seen := make(map[string]bool)
		var lastOffset int64 = 0
		for len(seen) < total {
			fresp, err := client.Fetch(ctx, &proto.FetchRequest{
				Topic:       "events",
				Partition:   0,
				Offset:      lastOffset,
				MaxMessages: 10,
			})
			if err != nil {
				t.Errorf("Fetch error: %v", err)
				return
			}
			for _, rec := range fresp.Records {
				seen[string(rec.Message.Key)] = true
				lastOffset = rec.Offset + 1
			}
			time.Sleep(20 * time.Millisecond)
		}
		if len(seen) != total {
			t.Errorf("expected %d unique keys got %d", total, len(seen))
		}
	}()

	wg.Wait()
	fetchWg.Wait()
}
