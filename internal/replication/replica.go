package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"telemetry-pipeline/proto"

	"google.golang.org/grpc"
)

type Follower struct {
	ID   string
	Addr string
	cli  proto.ReplicatorClient
}

type Replicator struct {
	Followers []*Follower
	Acks      int // number of required acknowledgements
	mu        sync.Mutex
}

// NewReplicator creates a replicator with acks requirement (0, 1, N, all).
func NewReplicator(followers map[string]string, tls grpc.DialOption, acks int) (*Replicator, error) {
	flw := make([]*Follower, 0, len(followers))
	for id, addr := range followers {
		conn, err := grpc.Dial(addr, tls, grpc.WithBlock())
		if err != nil {
			return nil, fmt.Errorf("dial follower %s: %w", id, err)
		}
		flw = append(flw, &Follower{
			ID:   id,
			Addr: addr,
			cli:  proto.NewReplicatorClient(conn),
		})
	}
	return &Replicator{Followers: flw, Acks: acks}, nil
}

func (r *Replicator) Append(ctx context.Context, req *proto.ReplicateRequest) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(r.Followers))
	acks := 1 // leader itself
	total := len(r.Followers) + 1

	for _, f := range r.Followers {
		wg.Add(1)
		go func(f *Follower) {
			defer wg.Done()
			ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, err := f.cli.Append(ctx2, req)
			if err != nil {
				fmt.Println("error in Append", f.Addr, f.ID, err, req.Partition)
				errs <- err
				return
			}
			errs <- nil
		}(f)
	}

	wg.Wait()
	close(errs)

	for e := range errs {
		if e == nil {
			acks++
		} else {
			fmt.Println(e)
		}
	}

	// Special cases: 0 = no wait, 1 = leader only, n = custom, total = all
	if r.Acks == 0 {
		return nil // fire-and-forget
	}
	if r.Acks > total {
		return fmt.Errorf("invalid acks=%d > total replicas=%d", r.Acks, total)
	}

	if acks >= r.Acks {
		return nil
	}
	return fmt.Errorf("acks=%d < required=%d", acks, r.Acks)
}
