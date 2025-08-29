package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"telemetry-pipeline/internal/cluster"
	pb "telemetry-pipeline/proto"
)

type Consumer struct {
	meta *cluster.MetadataManager
}

func NewConsumer(bootstrap string) *Consumer {
	return &Consumer{meta: cluster.NewMetadataManager(bootstrap)}
}

func (c *Consumer) Fetch(topic string, partition int, offset int64, maxMsgs int32) (*pb.FetchResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// step 1: resolve leader
	addr, err := c.meta.GetOrRefreshLeader(ctx, topic, partition)
	if err != nil {
		return nil, err
	}

	client, err := c.meta.GetClient(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("dial leader %s failed: %w", addr, err)
	}

	req := &pb.FetchRequest{
		Topic:       topic,
		Partition:   int32(partition),
		Offset:      offset,
		MaxMessages: maxMsgs,
	}

	resp, err := client.Fetch(ctx, req)
	if err != nil {
		// step 2: handle "not leader"
		newAddr, retry, herr := c.meta.HandleNotLeader(ctx, topic, partition, err)
		if herr != nil {
			return nil, herr
		}
		if retry {
			log.Printf("redirected fetch to %s", newAddr)
			client, err = c.meta.GetClient(ctx, newAddr)
			if err == nil {
				resp, err = client.Fetch(ctx, req)
				if err != nil {
					return nil, fmt.Errorf("retry fetch failed: %w", err)
				}
			}
		} else {
			return nil, err
		}
	}

	return resp, nil
}

func (c *Consumer) Close() {
	c.meta.Close()
}

func (c *Consumer) Meta() *cluster.MetadataManager {
	return c.meta
}
