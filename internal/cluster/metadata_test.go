package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pb "telemetry-pipeline/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// ---- Fake gRPC MQClient ----
type fakeMQClient struct {
	metadataResp *pb.MetadataResponse
	metadataErr  error
}

func (f *fakeMQClient) Metadata(ctx context.Context, in *pb.MetadataRequest, opts ...grpc.CallOption) (*pb.MetadataResponse, error) {
	if f.metadataErr != nil {
		return nil, f.metadataErr
	}
	return f.metadataResp, nil
}
func (f *fakeMQClient) Produce(ctx context.Context, in *pb.ProduceRequest, opts ...grpc.CallOption) (*pb.ProduceResponse, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeMQClient) Fetch(ctx context.Context, in *pb.FetchRequest, opts ...grpc.CallOption) (*pb.FetchResponse, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeMQClient) CreateTopic(ctx context.Context, in *pb.CreateTopicRequest, opts ...grpc.CallOption) (*pb.CreateTopicResponse, error) {
	return nil, errors.New("not implemented")
}
func (m *fakeMQClient) DeleteTopic(ctx context.Context, in *pb.DeleteTopicRequest, opts ...grpc.CallOption) (*pb.DeleteTopicResponse, error) {
	return nil, errors.New("not implemented")
}
func (m *fakeMQClient) CommitOffsets(ctx context.Context, in *pb.CommitOffsetsRequest, opts ...grpc.CallOption) (*pb.CommitOffsetsResponse, error) {
	return nil, errors.New("not implemented")
}
func (m *fakeMQClient) FetchOffsets(ctx context.Context, in *pb.FetchOffsetsRequest, opts ...grpc.CallOption) (*pb.FetchOffsetsResponse, error) {
	return nil, errors.New("not implemented")
}

// ---- Test Helpers ----

// injectFake replaces MetadataManager.clients[bootstrap] with fake
func injectFake(m *MetadataManager, cli pb.MQClient) {
	mu := &m.mu
	mu.Lock()
	defer mu.Unlock()
	m.clients[m.bootstrap] = cli
}

// ---- Tests ----

func TestRefreshMetadata_Positive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	m := NewMetadataManager("bootstrap:9092")

	// Fake response: 1 broker, 1 partition
	fake := &fakeMQClient{
		metadataResp: &pb.MetadataResponse{
			Brokers: []*pb.Broker{{Id: "b1", Host: "127.0.0.1", Port: 9092}},
			Partitions: []*pb.TopicPartitionMeta{{
				Topic:     "t1",
				Partition: 0,
				Leader:    "b1",
			}},
		},
	}
	injectFake(m, fake)

	err := m.RefreshMetadata(ctx, "t1")
	require.NoError(t, err)

	// leaderMap should be populated
	addr := m.LookupLeader("t1", 0)
	assert.Equal(t, "127.0.0.1:9092", addr)
}

func TestRefreshMetadata_BrokerMissingLeader(t *testing.T) {
	ctx := context.Background()
	m := NewMetadataManager("bootstrap:9092")

	// Partition leader "bX" not in Brokers list
	fake := &fakeMQClient{
		metadataResp: &pb.MetadataResponse{
			Brokers: []*pb.Broker{{Id: "b1", Host: "127.0.0.1", Port: 9092}},
			Partitions: []*pb.TopicPartitionMeta{{
				Topic:     "t1",
				Partition: 0,
				Leader:    "bX",
			}},
		},
	}
	injectFake(m, fake)

	err := m.RefreshMetadata(ctx, "t1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "leader bX not in broker map")
}

func TestRefreshMetadata_ClientError(t *testing.T) {
	ctx := context.Background()
	m := NewMetadataManager("bootstrap:9092")

	fake := &fakeMQClient{metadataErr: errors.New("metadata failure")}
	injectFake(m, fake)

	err := m.RefreshMetadata(ctx, "t1")
	assert.Error(t, err)
}

func TestLookupLeader_NoEntry(t *testing.T) {
	m := NewMetadataManager("bootstrap:9092")
	addr := m.LookupLeader("t1", 0)
	assert.Empty(t, addr)
}

func TestGetOrRefreshLeader_Positive(t *testing.T) {
	ctx := context.Background()
	m := NewMetadataManager("bootstrap:9092")

	fake := &fakeMQClient{
		metadataResp: &pb.MetadataResponse{
			Brokers: []*pb.Broker{{Id: "b1", Host: "localhost", Port: 9092}},
			Partitions: []*pb.TopicPartitionMeta{{
				Topic:     "t1",
				Partition: 1,
				Leader:    "b1",
			}},
		},
	}
	injectFake(m, fake)

	addr, err := m.GetOrRefreshLeader(ctx, "t1", 1)
	require.NoError(t, err)
	assert.Equal(t, "localhost:9092", addr)
}

func TestGetOrRefreshLeader_NoLeaderEvenAfterRefresh(t *testing.T) {
	ctx := context.Background()
	m := NewMetadataManager("bootstrap:9092")

	fake := &fakeMQClient{
		metadataResp: &pb.MetadataResponse{ // empty
			Brokers:    []*pb.Broker{},
			Partitions: []*pb.TopicPartitionMeta{},
		},
	}
	injectFake(m, fake)

	addr, err := m.GetOrRefreshLeader(ctx, "t1", 0)
	assert.Error(t, err)
	assert.Empty(t, addr)
}

func TestHandleNotLeader_Positive(t *testing.T) {
	ctx := context.Background()
	m := NewMetadataManager("bootstrap:9092")

	fake := &fakeMQClient{
		metadataResp: &pb.MetadataResponse{
			Brokers: []*pb.Broker{{Id: "b1", Host: "host", Port: 1234}},
			Partitions: []*pb.TopicPartitionMeta{{
				Topic:     "t1",
				Partition: 0,
				Leader:    "b1",
			}},
		},
	}
	injectFake(m, fake)

	addr, retry, err := m.HandleNotLeader(ctx, "t1", 0, fmt.Errorf("rpc error: not leader"))
	require.NoError(t, err)
	assert.True(t, retry)
	assert.Equal(t, "host:1234", addr)
}

func TestHandleNotLeader_StillNoLeader(t *testing.T) {
	ctx := context.Background()
	m := NewMetadataManager("bootstrap:9092")

	fake := &fakeMQClient{
		metadataResp: &pb.MetadataResponse{}, // no brokers/partitions
	}
	injectFake(m, fake)

	addr, retry, err := m.HandleNotLeader(ctx, "t1", 0, fmt.Errorf("not leader"))
	assert.Error(t, err)
	assert.Empty(t, addr)
	assert.False(t, retry)
}

func TestHandleNotLeader_UnrelatedError(t *testing.T) {
	ctx := context.Background()
	m := NewMetadataManager("bootstrap:9092")

	addr, retry, err := m.HandleNotLeader(ctx, "t1", 0, errors.New("network down"))
	assert.Error(t, err)
	assert.False(t, retry)
	assert.Empty(t, addr)
}

func TestClose(t *testing.T) {
	m := NewMetadataManager("bootstrap:9092")
	// simulate a dummy connection in map
	m.conns = map[string]*grpc.ClientConn{} // you’d normally inject a real *grpc.ClientConn
	// safe to just call Close()
	m.Close()
}
