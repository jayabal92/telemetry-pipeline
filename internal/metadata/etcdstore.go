package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path"
	"sort"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Store struct {
	// c *clientv3.Client
	c ClientAPI
}

type ClientAPI interface {
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
}

type StoreAPI interface {
	RegisterBroker(ctx context.Context, id, addr string, ttl int64) error
	ListBrokers(ctx context.Context) (map[string]string, error)
	GetTopic(ctx context.Context, t string) (*TopicDesc, error)
	GetPartitionState(ctx context.Context, t string, p int) (*PartitionState, error)
	CreateTopic(ctx context.Context, td TopicDesc) error
	AssignInitialLeaders(ctx context.Context, topic string, parts, rf int) error
	CommitOffset(ctx context.Context, group, topic string, part int, off int64) error
	FetchGroupOffsets(ctx context.Context, group, topic string) ([]GroupPartitionOffset, error)
}

type TopicDesc struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	RF         int    `json:"rf"`
}

type PartitionState struct {
	Topic       string   `json:"topic"`
	Partition   int      `json:"partition"`
	Leader      string   `json:"leader"`
	ISR         []string `json:"isr"`
	LeaderEpoch int64    `json:"leader_epoch"`
}

func NewStore(c ClientAPI) *Store { return &Store{c: c} }

func topicKey(t string) string       { return path.Join("/mq/topics", t) }
func partKey(t string, p int) string { return path.Join("/mq/partitions", fmt.Sprintf("%s-%d", t, p)) }
func brokersKey() string             { return "/mq/brokers" }
func brokerKey(id string) string     { return path.Join(brokersKey(), id) }
func groupKey(g string) string       { return path.Join("/mq/groups", g) }

func (s *Store) RegisterBroker(ctx context.Context, id, addr string, ttlSec int64) error {
	// ephemeral lease for liveness
	lease, err := s.c.Grant(ctx, ttlSec)
	if err != nil {
		return err
	}

	_, err = s.c.Put(ctx, brokerKey(id), addr, clientv3.WithLease(lease.ID))
	if err != nil {
		return err
	}
	// keepalive in background
	ch, kaerr := s.c.KeepAlive(ctx, lease.ID)
	if kaerr != nil {
		return kaerr
	}
	go func() {
		for range ch { /* drain */
		}
	}()
	return nil
}

func (s *Store) ListBrokers(ctx context.Context) (map[string]string, error) {
	log.Printf("into Listbrokers")
	resp, err := s.c.Get(ctx, brokersKey(), clientv3.WithPrefix())
	if err != nil {
		log.Println("Error fetching brokers", err)
		return nil, err
	}
	m := map[string]string{}
	for _, kv := range resp.Kvs {
		m[path.Base(string(kv.Key))] = string(kv.Value)
	}
	return m, nil
}

func (s *Store) CreateTopic(ctx context.Context, t TopicDesc) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = s.c.Put(ctx, topicKey(t.Name), string(b))
	return err
}

func (s *Store) GetTopic(ctx context.Context, name string) (*TopicDesc, error) {
	resp, err := s.c.Get(ctx, topicKey(name))
	if err != nil || len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("topic not found")
	}
	var td TopicDesc
	if err := json.Unmarshal(resp.Kvs[0].Value, &td); err != nil {
		return nil, err
	}
	return &td, nil
}

func (s *Store) SetPartitionState(ctx context.Context, st PartitionState) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	_, err = s.c.Put(ctx, partKey(st.Topic, st.Partition), string(b))
	return err
}

func (s *Store) GetPartitionState(ctx context.Context, t string, p int) (*PartitionState, error) {
	resp, err := s.c.Get(ctx, partKey(t, p))
	if err != nil || len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("partition not found")
	}
	var st PartitionState
	if err := json.Unmarshal(resp.Kvs[0].Value, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *Store) AssignInitialLeaders(ctx context.Context, t string, partitions, rf int) error {
	brokers, err := s.ListBrokers(ctx)
	if err != nil {
		return err
	}
	if len(brokers) < rf {
		return fmt.Errorf("not enough brokers for rf=%d", rf)
	}
	// stable iteration order
	ids := make([]string, 0, len(brokers))
	for id := range brokers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for p := 0; p < partitions; p++ {
		isr := make([]string, 0, rf)
		for i := 0; i < rf; i++ {
			isr = append(isr, ids[(p+i)%len(ids)])
		}
		st := PartitionState{Topic: t, Partition: p, Leader: isr[0], ISR: isr, LeaderEpoch: time.Now().UnixNano()}
		if err := s.SetPartitionState(ctx, st); err != nil {
			return err
		}
	}
	return nil
}

// Consumer group offsets

type GroupPartitionOffset struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

func (s *Store) CommitOffset(ctx context.Context, group, topic string, part int, off int64) error {
	k := path.Join(groupKey(group), topic, fmt.Sprintf("%d", part))
	_, err := s.c.Put(ctx, k, fmt.Sprintf("%d", off))
	return err
}

func (s *Store) FetchGroupOffsets(ctx context.Context, group, topic string) ([]GroupPartitionOffset, error) {
	k := path.Join(groupKey(group), topic)
	resp, err := s.c.Get(ctx, k, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	res := make([]GroupPartitionOffset, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var part int
		var off int64
		fmt.Sscanf(path.Base(string(kv.Key)), "%d", &part)
		fmt.Sscanf(string(kv.Value), "%d", &off)
		res = append(res, GroupPartitionOffset{Partition: part, Offset: off})
	}
	return res, nil
}
