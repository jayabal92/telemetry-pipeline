package producer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	pb "telemetry-pipeline/proto"

	"google.golang.org/grpc"
)

type Producer struct {
	mu        sync.RWMutex
	conns     map[string]*grpc.ClientConn // addr → conn
	clients   map[string]pb.MQClient      // addr → client
	leaderMap map[string]map[int]string   // topic → partition → leaderAddr
	bootstrap string                      // bootstrap broker
	dialOpts  []grpc.DialOption
}

// NewProducer creates a producer with a bootstrap broker.
func NewProducer(bootstrapAddr string) *Producer {
	return &Producer{
		conns:     make(map[string]*grpc.ClientConn),
		clients:   make(map[string]pb.MQClient),
		leaderMap: make(map[string]map[int]string),
		bootstrap: bootstrapAddr,
		dialOpts:  []grpc.DialOption{grpc.WithInsecure()},
	}
}

func (p *Producer) getClient(ctx context.Context, addr string) (pb.MQClient, error) {
	p.mu.RLock()
	client, ok := p.clients[addr]
	p.mu.RUnlock()
	if ok {
		return client, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// double check
	if client, ok = p.clients[addr]; ok {
		return client, nil
	}

	conn, err := grpc.DialContext(ctx, addr, append(p.dialOpts, grpc.WithBlock())...)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = conn
	client = pb.NewMQClient(conn)
	p.clients[addr] = client
	return client, nil
}

// refreshMetadata queries the cluster and updates leaderMap.
func (p *Producer) refreshMetadata(ctx context.Context, topic string) error {
	client, err := p.getClient(ctx, p.bootstrap)
	if err != nil {
		return err
	}

	md, err := client.Metadata(ctx, &pb.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return err
	}

	// Step 1: build brokerID → host:port map
	brokerMap := make(map[string]string)
	for _, b := range md.Brokers {
		brokerMap[b.Id] = fmt.Sprintf("%s:%d", b.Host, b.Port)
	}

	// Step 2: build topic partition → leaderAddr map
	newMap := make(map[int]string)
	for _, part := range md.Partitions {
		if part.Topic != topic {
			continue
		}
		addr, ok := brokerMap[part.Leader]
		if !ok {
			return fmt.Errorf("leader %s not found in broker map", part.Leader)
		}
		newMap[int(part.Partition)] = addr
	}

	p.mu.Lock()
	p.leaderMap[topic] = newMap
	p.mu.Unlock()

	return nil
}

// Produce sends a batch of messages to the leader for a partition.
func (p *Producer) Produce(topic string, partition int, msgs []*pb.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addr := p.lookupLeader(topic, partition)
	if addr == "" {
		if err := p.refreshMetadata(ctx, topic); err != nil {
			return fmt.Errorf("metadata lookup failed: %w", err)
		}
		addr = p.lookupLeader(topic, partition)
		if addr == "" {
			return fmt.Errorf("no leader found for %s-%d", topic, partition)
		}
	}

	log.Printf("Produce client address: %s", addr)
	client, err := p.getClient(ctx, addr)
	if err != nil {
		return fmt.Errorf("dial leader %s failed: %w", addr, err)
	}

	req := &pb.ProduceRequest{
		Topic:     topic,
		Partition: int32(partition),
		Messages:  msgs,
		Acks:      pb.ProduceRequest_LEADER,
	}

	resp, err := client.Produce(ctx, req)
	if err == nil {
		log.Printf("produced batch to %s-%d leader=%s count=%d lastOffset=%d",
			topic, partition, addr, len(resp.Offsets), resp.Offsets[len(resp.Offsets)-1])
		return nil
	}
	log.Printf("error in produce: %v", err)
	// handle not leader case
	if strings.Contains(err.Error(), "not leader") {
		log.Printf("produce redirect: %v → refreshing metadata", err)
		if rerr := p.refreshMetadata(ctx, topic); rerr != nil {
			return fmt.Errorf("produce failed, metadata refresh failed: %w", rerr)
		}
		// retry once
		newAddr := p.lookupLeader(topic, partition)
		if newAddr == "" {
			return fmt.Errorf("still no leader for %s-%d", topic, partition)
		}
		client, err = p.getClient(ctx, newAddr)
		if err != nil {
			return fmt.Errorf("dial leader %s failed: %w", newAddr, err)
		}
		resp, err = client.Produce(ctx, req)
		if err != nil {
			return fmt.Errorf("retry produce failed: %w", err)
		}
		log.Printf("produced batch (retry) to %s-%d leader=%s count=%d lastOffset=%d",
			topic, partition, newAddr, len(resp.Offsets), resp.Offsets[len(resp.Offsets)-1])
		return nil
	}

	return fmt.Errorf("produce RPC error: %w", err)
}

func (p *Producer) lookupLeader(topic string, partition int) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if parts, ok := p.leaderMap[topic]; ok {
		if addr, ok := parts[partition]; ok {
			return addr
		}
	}
	return ""
}

// Close all gRPC connections.
func (p *Producer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, conn := range p.conns {
		_ = conn.Close()
		delete(p.conns, addr)
		delete(p.clients, addr)
	}
}
