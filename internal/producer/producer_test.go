package producer

import (
	"context"
	"errors"
	"testing"

	pb "telemetry-pipeline/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// ---- mock MQClient ----
type mockMQClient struct {
	metadataResp *pb.MetadataResponse
	metadataErr  error
	produceResp  *pb.ProduceResponse
	produceErr   error
}

func (m *mockMQClient) Produce(ctx context.Context, in *pb.ProduceRequest, opts ...grpc.CallOption) (*pb.ProduceResponse, error) {
	return m.produceResp, m.produceErr
}
func (m *mockMQClient) Metadata(ctx context.Context, in *pb.MetadataRequest, opts ...grpc.CallOption) (*pb.MetadataResponse, error) {
	return m.metadataResp, m.metadataErr
}

// ---- unused methods (stubbed for interface compliance) ----
func (m *mockMQClient) Fetch(ctx context.Context, in *pb.FetchRequest, opts ...grpc.CallOption) (*pb.FetchResponse, error) {
	return nil, errors.New("not implemented")
}
func (m *mockMQClient) CreateTopic(ctx context.Context, in *pb.CreateTopicRequest, opts ...grpc.CallOption) (*pb.CreateTopicResponse, error) {
	return nil, errors.New("not implemented")
}
func (m *mockMQClient) DeleteTopic(ctx context.Context, in *pb.DeleteTopicRequest, opts ...grpc.CallOption) (*pb.DeleteTopicResponse, error) {
	return nil, errors.New("not implemented")
}
func (m *mockMQClient) CommitOffsets(ctx context.Context, in *pb.CommitOffsetsRequest, opts ...grpc.CallOption) (*pb.CommitOffsetsResponse, error) {
	return nil, errors.New("not implemented")
}
func (m *mockMQClient) FetchOffsets(ctx context.Context, in *pb.FetchOffsetsRequest, opts ...grpc.CallOption) (*pb.FetchOffsetsResponse, error) {
	return nil, errors.New("not implemented")
}

// ---- Tests ----

// ✅ Positive case: produce works first try
func TestProduce_Success(t *testing.T) {
	p := NewProducer("bootstrap:9092")

	// Setup leader map directly (simulate refresh worked)
	p.leaderMap["topicA"] = map[int]string{0: "leader1"}
	p.clients["leader1"] = &mockMQClient{
		produceResp: &pb.ProduceResponse{
			Offsets: []int64{10, 11},
		},
	}

	msgs := []*pb.Message{{Key: []byte("k1"), Value: []byte("v1")}}
	err := p.Produce("topicA", 0, msgs)
	require.NoError(t, err)
}

// 🔴 Metadata fetch fails
func TestProduce_MetadataFails(t *testing.T) {
	p := NewProducer("bootstrap:9092")
	p.clients["bootstrap:9092"] = &mockMQClient{
		metadataErr: errors.New("boom"),
	}

	err := p.Produce("topicA", 0, []*pb.Message{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "metadata lookup failed")
}

// 🔴 Leader missing from broker map
func TestProduce_LeaderMissing(t *testing.T) {
	p := NewProducer("bootstrap:9092")
	p.clients["bootstrap:9092"] = &mockMQClient{
		metadataResp: &pb.MetadataResponse{
			Brokers:    []*pb.Broker{{Id: "b1", Host: "h1", Port: 1234}},
			Partitions: []*pb.TopicPartitionMeta{{Topic: "topicA", Partition: 0, Leader: "missing"}},
		},
	}

	err := p.Produce("topicA", 0, []*pb.Message{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "leader missing not found")
}

// 🔴 Produce RPC error (not "not leader")
func TestProduce_RPCError(t *testing.T) {
	p := NewProducer("bootstrap:9092")

	p.leaderMap["topicA"] = map[int]string{0: "addr1"}
	p.clients["addr1"] = &mockMQClient{
		produceErr: errors.New("network down"),
	}

	err := p.Produce("topicA", 0, []*pb.Message{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "produce RPC error")
}

// 🔴 Produce fails with "not leader", retry also fails
func TestProduce_NotLeader_RetryFails(t *testing.T) {
	p := NewProducer("bootstrap:9092")

	// First leader returns "not leader"
	first := &mockMQClient{produceErr: errors.New("not leader")}
	// Retry leader returns another error
	second := &mockMQClient{produceErr: errors.New("still bad")}

	p.leaderMap["topicA"] = map[int]string{0: "oldLeader"}
	p.clients["oldLeader"] = first
	p.clients["bootstrap:9092"] = &mockMQClient{
		metadataResp: &pb.MetadataResponse{
			Brokers:    []*pb.Broker{{Id: "b2", Host: "h2", Port: 9999}},
			Partitions: []*pb.TopicPartitionMeta{{Topic: "topicA", Partition: 0, Leader: "b2"}},
		},
	}
	p.clients["h2:9999"] = second

	err := p.Produce("topicA", 0, []*pb.Message{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "retry produce failed")
}

// ✅ Positive case: not leader first, retry succeeds
func TestProduce_NotLeader_RetrySuccess(t *testing.T) {
	p := NewProducer("bootstrap:9092")

	// First attempt fails
	first := &mockMQClient{produceErr: errors.New("not leader")}
	// Retry works
	second := &mockMQClient{produceResp: &pb.ProduceResponse{
		Offsets: []int64{100},
	}}

	p.leaderMap["topicA"] = map[int]string{0: "oldLeader"}
	p.clients["oldLeader"] = first
	p.clients["bootstrap:9092"] = &mockMQClient{
		metadataResp: &pb.MetadataResponse{
			Brokers:    []*pb.Broker{{Id: "b2", Host: "h2", Port: 9999}},
			Partitions: []*pb.TopicPartitionMeta{{Topic: "topicA", Partition: 0, Leader: "b2"}},
		},
	}
	p.clients["h2:9999"] = second

	msgs := []*pb.Message{{Key: []byte("k1"), Value: []byte("v1")}}
	err := p.Produce("topicA", 0, msgs)
	require.NoError(t, err)
}
