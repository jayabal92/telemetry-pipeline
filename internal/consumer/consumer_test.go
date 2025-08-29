package consumer_test

import (
	"context"
	"errors"
	"testing"

	"telemetry-pipeline/internal/cluster"
	"telemetry-pipeline/internal/consumer"
	pb "telemetry-pipeline/proto"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// ---- fake MQClient ----
type stubMQClient struct {
	fetchFn func(ctx context.Context, in *pb.FetchRequest, opts ...grpc.CallOption) (*pb.FetchResponse, error)
}

func (s *stubMQClient) Fetch(ctx context.Context, in *pb.FetchRequest, opts ...grpc.CallOption) (*pb.FetchResponse, error) {
	return s.fetchFn(ctx, in, opts...)
}
func (s *stubMQClient) Produce(ctx context.Context, in *pb.ProduceRequest, opts ...grpc.CallOption) (*pb.ProduceResponse, error) {
	return nil, errors.New("not implemented")
}
func (s *stubMQClient) CreateTopic(ctx context.Context, in *pb.CreateTopicRequest, opts ...grpc.CallOption) (*pb.CreateTopicResponse, error) {
	return nil, errors.New("not implemented")
}
func (s *stubMQClient) Metadata(ctx context.Context, in *pb.MetadataRequest, opts ...grpc.CallOption) (*pb.MetadataResponse, error) {
	return nil, errors.New("not implemented")
}
func (s *stubMQClient) DeleteTopic(ctx context.Context, in *pb.DeleteTopicRequest, opts ...grpc.CallOption) (*pb.DeleteTopicResponse, error) {
	return nil, errors.New("not implemented")
}
func (s *stubMQClient) CommitOffsets(ctx context.Context, in *pb.CommitOffsetsRequest, opts ...grpc.CallOption) (*pb.CommitOffsetsResponse, error) {
	return nil, errors.New("not implemented")
}
func (s *stubMQClient) FetchOffsets(ctx context.Context, in *pb.FetchOffsetsRequest, opts ...grpc.CallOption) (*pb.FetchOffsetsResponse, error) {
	return nil, errors.New("not implemented")
}

// helper to inject fake MQClient into MetadataManager
func injectFakeClient(m *cluster.MetadataManager, addr string, c pb.MQClient) {
	m.InjectClient(addr, c)
}

// ---- tests ----

func TestConsumer_Fetch_Success(t *testing.T) {
	con := consumer.NewConsumer("bootstrap:1234")
	m := con.Meta()

	// initial metadata: leader=broker1
	cluster.RefreshMetadataHook = func(ctx context.Context, mm *cluster.MetadataManager, topic string) error {
		mm.SetLeaderMap(map[string]map[int]string{"testTopic": {0: "broker1"}})
		return nil
	}
	defer func() { cluster.RefreshMetadataHook = nil }()

	// broker1 always succeeds
	fakeClient := &stubMQClient{
		fetchFn: func(ctx context.Context, in *pb.FetchRequest, opts ...grpc.CallOption) (*pb.FetchResponse, error) {
			return &pb.FetchResponse{Records: []*pb.FetchedMessage{
				&pb.FetchedMessage{Message: &pb.Message{
					Value: []byte("hello"),
				}},
			}}, nil
		},
	}
	injectFakeClient(m, "broker1", fakeClient)

	resp, err := con.Fetch("testTopic", 0, 0, 5)
	require.NoError(t, err)
	require.Equal(t, "hello", string(resp.Records[0].Message.Value))
}

func TestConsumer_Fetch_FailsAfterRetry(t *testing.T) {
	con := consumer.NewConsumer("bootstrap:1234")
	m := con.Meta()

	cluster.RefreshMetadataHook = func(ctx context.Context, mm *cluster.MetadataManager, topic string) error {
		mm.SetLeaderMap(map[string]map[int]string{"testTopic": {0: "broker1"}})
		return nil
	}
	defer func() { cluster.RefreshMetadataHook = nil }()

	// broker1 always errors
	fakeClient := &stubMQClient{
		fetchFn: func(ctx context.Context, in *pb.FetchRequest, opts ...grpc.CallOption) (*pb.FetchResponse, error) {
			return nil, errors.New("fatal fetch error")
		},
	}
	injectFakeClient(m, "broker1", fakeClient)

	_, err := con.Fetch("testTopic", 0, 0, 5)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fatal fetch error")
}

func TestConsumer_Fetch_NotLeaderThenRetry(t *testing.T) {
	con := consumer.NewConsumer("bootstrap:1234")
	m := con.Meta()

	// first metadata → broker1
	cluster.RefreshMetadataHook = func(ctx context.Context, mm *cluster.MetadataManager, topic string) error {
		mm.SetLeaderMap(map[string]map[int]string{"testTopic": {0: "broker1"}})
		return nil
	}

	// after retry, refresh → broker2
	called := false
	cluster.RefreshMetadataHook = func(ctx context.Context, mm *cluster.MetadataManager, topic string) error {
		if !called {
			mm.SetLeaderMap(map[string]map[int]string{"testTopic": {0: "broker1"}})
			called = true
		} else {
			mm.SetLeaderMap(map[string]map[int]string{"testTopic": {0: "broker2"}})
		}
		return nil
	}
	defer func() { cluster.RefreshMetadataHook = nil }()

	// broker1 → not leader error
	broker1 := &stubMQClient{
		fetchFn: func(ctx context.Context, in *pb.FetchRequest, opts ...grpc.CallOption) (*pb.FetchResponse, error) {
			return nil, errors.New("not leader: leader=broker2")
		},
	}
	// broker2 → success
	broker2 := &stubMQClient{
		fetchFn: func(ctx context.Context, in *pb.FetchRequest, opts ...grpc.CallOption) (*pb.FetchResponse, error) {

			return &pb.FetchResponse{Records: []*pb.FetchedMessage{
				&pb.FetchedMessage{Message: &pb.Message{
					Value: []byte("from-broker2"),
				}},
			}}, nil
		},
	}

	injectFakeClient(m, "broker1", broker1)
	injectFakeClient(m, "broker2", broker2)

	resp, err := con.Fetch("testTopic", 0, 0, 5)
	require.NoError(t, err)
	require.Equal(t, "from-broker2", string(resp.Records[0].Message.Value))
}
