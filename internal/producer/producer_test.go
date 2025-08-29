package producer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"telemetry-pipeline/internal/cluster"
	"telemetry-pipeline/internal/producer"
	pb "telemetry-pipeline/proto"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// ---- unified fake MQClient ----
type stubMQClient struct {
	produceFn     func(ctx context.Context, in *pb.ProduceRequest, opts ...grpc.CallOption) (*pb.ProduceResponse, error)
	createTopicFn func(ctx context.Context, in *pb.CreateTopicRequest, opts ...grpc.CallOption) (*pb.CreateTopicResponse, error)
	metadataFn    func(ctx context.Context, in *pb.MetadataRequest, opts ...grpc.CallOption) (*pb.MetadataResponse, error)
}

func (s *stubMQClient) Produce(ctx context.Context, in *pb.ProduceRequest, opts ...grpc.CallOption) (*pb.ProduceResponse, error) {
	if s.produceFn == nil {
		return nil, errors.New("Produce not implemented")
	}
	return s.produceFn(ctx, in, opts...)
}
func (s *stubMQClient) CreateTopic(ctx context.Context, in *pb.CreateTopicRequest, opts ...grpc.CallOption) (*pb.CreateTopicResponse, error) {
	if s.createTopicFn == nil {
		return nil, errors.New("CreateTopic not implemented")
	}
	return s.createTopicFn(ctx, in, opts...)
}
func (s *stubMQClient) Metadata(ctx context.Context, in *pb.MetadataRequest, opts ...grpc.CallOption) (*pb.MetadataResponse, error) {
	if s.metadataFn == nil {
		return nil, errors.New("Metadata not implemented")
	}
	return s.metadataFn(ctx, in, opts...)
}

// unused methods
func (*stubMQClient) Fetch(ctx context.Context, in *pb.FetchRequest, opts ...grpc.CallOption) (*pb.FetchResponse, error) {
	return nil, errors.New("not implemented")
}
func (*stubMQClient) DeleteTopic(ctx context.Context, in *pb.DeleteTopicRequest, opts ...grpc.CallOption) (*pb.DeleteTopicResponse, error) {
	return nil, errors.New("not implemented")
}
func (*stubMQClient) CommitOffsets(ctx context.Context, in *pb.CommitOffsetsRequest, opts ...grpc.CallOption) (*pb.CommitOffsetsResponse, error) {
	return nil, errors.New("not implemented")
}
func (*stubMQClient) FetchOffsets(ctx context.Context, in *pb.FetchOffsetsRequest, opts ...grpc.CallOption) (*pb.FetchOffsetsResponse, error) {
	return nil, errors.New("not implemented")
}

// ---- test helpers ----

// configure leader + inject fake client
func setupLeaderAndClient(t *testing.T, prod *producer.Producer, topic string, partition int, broker string, client pb.MQClient) {
	m := prod.Meta()
	cluster.RefreshMetadataHook = func(ctx context.Context, mm *cluster.MetadataManager, t string) error {
		mm.SetLeaderMap(map[string]map[int]string{topic: {partition: broker}})
		return nil
	}
	t.Cleanup(func() { cluster.RefreshMetadataHook = nil })
	m.InjectClient(broker, client)
}

func TestProducer_Produce_Success(t *testing.T) {
	prod := producer.NewProducer("bootstrap:1234")

	client := &stubMQClient{
		produceFn: func(ctx context.Context, in *pb.ProduceRequest, opts ...grpc.CallOption) (*pb.ProduceResponse, error) {
			return &pb.ProduceResponse{Offsets: []int64{1, 2, 3}}, nil
		},
	}
	setupLeaderAndClient(t, prod, "testTopic", 0, "broker1", client)

	err := prod.Produce("testTopic", 0, []*pb.Message{{Value: []byte("hello")}})
	require.NoError(t, err)
}

func TestProducer_Produce_FailsAfterRetry(t *testing.T) {
	prod := producer.NewProducer("bootstrap:1234")

	client := &stubMQClient{
		produceFn: func(ctx context.Context, in *pb.ProduceRequest, opts ...grpc.CallOption) (*pb.ProduceResponse, error) {
			return nil, errors.New("fatal error")
		},
	}
	setupLeaderAndClient(t, prod, "testTopic", 0, "broker1", client)

	err := prod.Produce("testTopic", 0, []*pb.Message{{Value: []byte("fail")}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fatal error")
}

func TestProducer_Produce_NotLeaderThenRetry(t *testing.T) {
	prod := producer.NewProducer("bootstrap:1234")

	calls := 0
	client := &stubMQClient{
		produceFn: func(ctx context.Context, in *pb.ProduceRequest, opts ...grpc.CallOption) (*pb.ProduceResponse, error) {
			calls++
			if calls == 1 {
				return nil, fmt.Errorf("not leader: leader=broker2")
			}
			return &pb.ProduceResponse{Offsets: []int64{99}}, nil
		},
	}
	setupLeaderAndClient(t, prod, "testTopic", 0, "broker1", client)

	err := prod.Produce("testTopic", 0, []*pb.Message{{Value: []byte("retry-msg")}})
	require.NoError(t, err)
	require.Equal(t, 2, calls, "Produce should retry once after not leader")
}
func TestProducer_CreateTopic_Success(t *testing.T) {
	prod := producer.NewProducer("bootstrap:1234")
	m := prod.Meta()

	// avoid real network dial
	cluster.RefreshMetadataHook = func(ctx context.Context, mm *cluster.MetadataManager, topic string) error {
		mm.SetLeaderMap(map[string]map[int]string{topic: {0: "broker1"}})
		return nil
	}
	defer func() { cluster.RefreshMetadataHook = nil }()

	fakeClient := &stubMQClient{
		createTopicFn: func(ctx context.Context, in *pb.CreateTopicRequest, opts ...grpc.CallOption) (*pb.CreateTopicResponse, error) {
			require.Equal(t, "newTopic", in.Topic)
			require.Equal(t, int32(3), in.Partitions)
			return &pb.CreateTopicResponse{}, nil
		},
	}
	m.InjectClient("bootstrap:1234", fakeClient)

	err := prod.CreateTopic("newTopic", 3, 2)
	require.NoError(t, err)
}

func TestProducer_CreateTopic_Failure(t *testing.T) {
	prod := producer.NewProducer("bootstrap:1234")
	m := prod.Meta()

	// avoid real network dial
	cluster.RefreshMetadataHook = func(ctx context.Context, mm *cluster.MetadataManager, topic string) error {
		mm.SetLeaderMap(map[string]map[int]string{topic: {0: "broker1"}})
		return nil
	}
	defer func() { cluster.RefreshMetadataHook = nil }()

	fakeClient := &stubMQClient{
		createTopicFn: func(ctx context.Context, in *pb.CreateTopicRequest, opts ...grpc.CallOption) (*pb.CreateTopicResponse, error) {
			return nil, errors.New("create topic failed")
		},
	}
	m.InjectClient("bootstrap:1234", fakeClient)

	err := prod.CreateTopic("badTopic", 1, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "create topic failed")
}
