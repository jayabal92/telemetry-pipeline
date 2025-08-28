package replication

import (
	"context"
	"errors"
	"testing"

	"telemetry-pipeline/proto"

	"google.golang.org/grpc"
)

// Fake client implementing proto.ReplicatorClient
type fakeReplicatorClient struct {
	shouldFail bool
}

func (f *fakeReplicatorClient) Append(ctx context.Context, req *proto.ReplicateRequest, opts ...grpc.CallOption) (*proto.ReplicateResponse, error) {
	if f.shouldFail {
		return nil, errors.New("append failed")
	}
	return &proto.ReplicateResponse{}, nil
}

func TestAppend_AllSuccess(t *testing.T) {
	r := &Replicator{
		Followers: []*Follower{{ID: "f1", cli: &fakeReplicatorClient{}}},
		Acks:      1,
	}
	err := r.Append(context.Background(), &proto.ReplicateRequest{Partition: 1})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestAppend_FailureNotEnoughAcks(t *testing.T) {
	r := &Replicator{
		Followers: []*Follower{{ID: "f1", cli: &fakeReplicatorClient{shouldFail: true}}},
		Acks:      2, // requires more than leader ack
	}
	err := r.Append(context.Background(), &proto.ReplicateRequest{Partition: 1})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestAppend_FireAndForget(t *testing.T) {
	r := &Replicator{Followers: []*Follower{{ID: "f1", cli: &fakeReplicatorClient{}}}, Acks: 0}
	if err := r.Append(context.Background(), &proto.ReplicateRequest{}); err != nil {
		t.Fatalf("fire-and-forget should not fail: %v", err)
	}
}
