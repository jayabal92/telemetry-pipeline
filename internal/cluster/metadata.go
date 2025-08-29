package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync"

	pb "telemetry-pipeline/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var RefreshMetadataHook func(ctx context.Context, m *MetadataManager, topic string) error
var DialHook func(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error)

type MetadataManager struct {
	mu        sync.RWMutex
	conns     map[string]*grpc.ClientConn
	clients   map[string]pb.MQClient
	leaderMap map[string]map[int]string // topic → partition → leaderAddr
	bootstrap string
	dialOpts  []grpc.DialOption
}

func NewMetadataManager(bootstrap string, dialOpts ...grpc.DialOption) *MetadataManager {
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &MetadataManager{
		conns:     make(map[string]*grpc.ClientConn),
		clients:   make(map[string]pb.MQClient),
		leaderMap: make(map[string]map[int]string),
		bootstrap: bootstrap,
		dialOpts:  dialOpts,
	}
}

func (m *MetadataManager) GetClient(ctx context.Context, addr string) (pb.MQClient, error) {
	m.mu.RLock()
	client, ok := m.clients[addr]
	m.mu.RUnlock()
	if ok {
		return client, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// double-check
	if client, ok = m.clients[addr]; ok {
		return client, nil
	}

	conn, err := grpc.DialContext(ctx, addr, append(m.dialOpts, grpc.WithBlock())...)
	if err != nil {
		return nil, err
	}
	m.conns[addr] = conn
	client = pb.NewMQClient(conn)
	m.clients[addr] = client
	return client, nil
}

// RefreshMetadata updates the leader map for a topic.
func (m *MetadataManager) refreshMetadataInternal(ctx context.Context, topic string) error {
	client, err := m.GetClient(ctx, m.bootstrap)
	if err != nil {
		return err
	}

	md, err := client.Metadata(ctx, &pb.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return err
	}

	brokerMap := make(map[string]string)
	for _, b := range md.Brokers {
		brokerMap[b.Id] = fmt.Sprintf("%s:%d", b.Host, b.Port)
	}

	newMap := make(map[int]string)
	for _, part := range md.Partitions {
		if part.Topic != topic {
			continue
		}
		addr, ok := brokerMap[part.Leader]
		if !ok {
			return fmt.Errorf("leader %s not in broker map", part.Leader)
		}
		newMap[int(part.Partition)] = addr
	}

	m.mu.Lock()
	m.leaderMap[topic] = newMap
	m.mu.Unlock()

	return nil
}

// LookupLeader returns leader addr for topic+partition.
func (m *MetadataManager) LookupLeader(topic string, partition int) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if parts, ok := m.leaderMap[topic]; ok {
		if addr, ok := parts[partition]; ok {
			return addr
		}
	}
	return ""
}

// GetOrRefreshLeader ensures a leader address, refreshing if needed.
func (m *MetadataManager) GetOrRefreshLeader(ctx context.Context, topic string, partition int) (string, error) {
	addr := m.LookupLeader(topic, partition)
	if addr != "" {
		return addr, nil
	}
	if err := m.RefreshMetadata(ctx, topic); err != nil {
		return "", err
	}
	addr = m.LookupLeader(topic, partition)
	if addr == "" {
		return "", fmt.Errorf("no leader for %s-%d", topic, partition)
	}
	return addr, nil
}

// HandleNotLeader handles a "not leader" error by refreshing metadata and retrying.
func (m *MetadataManager) HandleNotLeader(ctx context.Context, topic string, partition int, rpcErr error) (string, bool, error) {
	if rpcErr == nil {
		return "", false, nil
	}
	if strings.Contains(rpcErr.Error(), "not leader") {
		if err := m.RefreshMetadata(ctx, topic); err != nil {
			return "", false, err
		}
		addr := m.LookupLeader(topic, partition)
		if addr == "" {
			return "", false, fmt.Errorf("still no leader for %s-%d", topic, partition)
		}
		return addr, true, nil
	}
	return "", false, rpcErr
}

// Close all connections.
func (m *MetadataManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for addr, conn := range m.conns {
		_ = conn.Close()
		delete(m.conns, addr)
		delete(m.clients, addr)
	}
}

// InjectClient allows tests to pre-load a MQClient for a broker address.
func (m *MetadataManager) InjectClient(addr string, client pb.MQClient) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.clients == nil {
		m.clients = make(map[string]pb.MQClient)
	}
	m.clients[addr] = client
}

// SetLeaderMap replaces the leader map (topic -> partition -> leaderAddr).
func (m *MetadataManager) SetLeaderMap(leaderMap map[string]map[int]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderMap = leaderMap
}

// GetLeaderMap returns a copy of the leader map (for testing/debugging).
func (m *MetadataManager) GetLeaderMap() map[string]map[int]string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]map[int]string)
	for topic, parts := range m.leaderMap {
		c := make(map[int]string)
		for p, addr := range parts {
			c[p] = addr
		}
		result[topic] = c
	}
	return result
}

// Patchable refresh for tests: can be overridden

func (m *MetadataManager) RefreshMetadata(ctx context.Context, topic string) error {
	if RefreshMetadataHook != nil {
		return RefreshMetadataHook(ctx, m, topic)
	}
	// keep original refresh logic here…
	return m.refreshMetadataInternal(ctx, topic)
}
