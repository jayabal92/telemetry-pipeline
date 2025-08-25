package mqclient

import (
	"context"
	"time"

	pb "telemetry-pipeline/proto"

	"google.golang.org/grpc"
)

type Client struct {
	conn *grpc.ClientConn
	cli  pb.MQClient
}

func Dial(addr string) (*Client, error) {
	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return &Client{conn: cc, cli: pb.NewMQClient(cc)}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) Produce(ctx context.Context, topic string, partition int32, msgs []*pb.Message) ([]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, err := c.cli.Produce(ctx, &pb.ProduceRequest{Topic: topic, Partition: partition, Messages: msgs})
	if err != nil {
		return nil, err
	}
	return res.Offsets, nil
}

// Fetch up to maxMessages starting at offset
func (c *Client) Fetch(ctx context.Context, topic string, partition int32, offset int64, max int32) ([]*pb.FetchedMessage, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, err := c.cli.Fetch(ctx, &pb.FetchRequest{Topic: topic, Partition: partition, Offset: offset, MaxMessages: max})
	if err != nil {
		return nil, 0, err
	}
	return res.Records, res.HighWatermark, nil
}
