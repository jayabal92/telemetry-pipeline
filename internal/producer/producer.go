package producer

import (
	"context"
	"fmt"
	"log"
	"time"

	"telemetry-pipeline/internal/cluster"
	pb "telemetry-pipeline/proto"
)

type Producer struct {
	bootstrap string // bootstrap broker
	meta      *cluster.MetadataManager
}

// NewProducer creates a producer with a bootstrap broker.
func NewProducer(bootstrapAddr string) *Producer {
	return &Producer{meta: cluster.NewMetadataManager(bootstrapAddr), bootstrap: bootstrapAddr}
}

// Produce sends a batch of messages to the leader for a partition.
func (p *Producer) Produce(topic string, partition int, msgs []*pb.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	addr, err := p.meta.GetOrRefreshLeader(ctx, topic, partition)
	if err != nil {
		return err
	}

	log.Printf("Produce client address: %s", addr)
	client, err := p.meta.GetClient(ctx, addr)
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

	newAddr, retry, herr := p.meta.HandleNotLeader(ctx, topic, partition, err)
	if herr != nil {
		return herr
	}
	log.Printf("error in produce: %v", err)
	if retry {
		client, _ = p.meta.GetClient(ctx, newAddr)
		resp, err = client.Produce(ctx, req)
		if err == nil {
			log.Printf("produced batch (retry) to %s-%d leader=%s count=%d lastOffset=%d",
				topic, partition, newAddr, len(resp.Offsets), resp.Offsets[len(resp.Offsets)-1])
			return nil
		}
	}

	return fmt.Errorf("produce RPC error: %w", err)
}

func (p *Producer) CreateTopic(topic string, numOfPartition, rf int32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := p.meta.GetClient(ctx, p.bootstrap)
	if err != nil {
		log.Printf("error connecting grpc server: %v", err)
		return err
	}

	_, err = client.CreateTopic(ctx, &pb.CreateTopicRequest{
		Topic:      topic,
		Partitions: numOfPartition,
		Rf:         rf,
	})
	if err != nil {
		log.Printf("unable to create topic: %v", err)
		return err
	}
	log.Printf("Topic creates succesfully: %s", topic)
	return nil
}

func (p *Producer) Close() {
	p.meta.Close()
}

func (p *Producer) Meta() *cluster.MetadataManager {
	return p.meta
}
