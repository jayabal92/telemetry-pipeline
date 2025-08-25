package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"telemetry-pipeline/internal/model"
	pb "telemetry-pipeline/proto"

	"google.golang.org/grpc"
)

// global atomic flag
var running int32

func main() {

	csvPath := getEnv("CSV_PATH", "./data/dcgm_metrics_20250718_134233.csv")
	serverAddr := getEnv("MQ_SERVER", "127.0.0.1:9092")
	topic := getEnv("MQ_TOPIC", "events")
	partition := getEnvInt("MQ_PARTITION", -1)
	batchSize := getEnvInt("BATCH_SIZE", 10)
	interval := getEnvInt("READ_INTERVAL", 10)

	// Connect to gRPC server
	conn, err := grpc.NewClient(serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()
	client := pb.NewMQClient(conn)

	// Ticker loop
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	log.Printf("starting producer: server=%s, topic=%s, batchSize=%d, interval=%ds",
		serverAddr, topic, batchSize, interval)

	// for {
	// 	// Run once immediately, then on each tick
	// 	if err := produceFromCSV(client, *csvPath, *topic, *partition, *batchSize); err != nil {
	// 		log.Printf("error producing from csv: %v", err)
	// 	}

	// 	<-ticker.C
	// }

	// run immediately at startup
	if atomic.CompareAndSwapInt32(&running, 0, 1) {
		go func() {
			defer atomic.StoreInt32(&running, 0)
			if err := produceFromCSV(client, csvPath, topic, partition, batchSize); err != nil {
				log.Printf("error producing from csv: %v", err)
			}
		}()
	} else {
		log.Println("previous job still running, skipping initial run")
	}

	for {
		select {
		case <-ticker.C:
			if !atomic.CompareAndSwapInt32(&running, 0, 1) {
				log.Println("previous job still running, skipping this tick")
				continue
			}
			go func() {
				defer atomic.StoreInt32(&running, 0)
				if err := produceFromCSV(client, csvPath, topic, partition, batchSize); err != nil {
					log.Printf("error producing from csv: %v", err)
				}
			}()
		}
	}
}

func produceFromCSV(client pb.MQClient, csvPath, topic string, partition, batchSize int) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open csv file: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)

	// Read header
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}
	headerIndex := make(map[string]int, len(headers))
	for i, h := range headers {
		headerIndex[h] = i
	}

	// Batch buffer
	batch := make([]*pb.Message, 0, batchSize)
	var rowCount int

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("error reading row: %v", err)
			continue
		}
		rowCount++

		t := model.Telemetry{
			Timestamp:  parseTime(safeGet(record, headerIndex, "timestamp")),
			MetricName: safeGet(record, headerIndex, "metric_name"),
			GPUId:      parseInt(safeGet(record, headerIndex, "gpu_id")),
			Device:     safeGet(record, headerIndex, "device"),
			UUID:       safeGet(record, headerIndex, "uuid"),
			ModelName:  safeGet(record, headerIndex, "modelname"),
			Hostname:   safeGet(record, headerIndex, "hostname"),
			Container:  safeGet(record, headerIndex, "container"),
			Pod:        safeGet(record, headerIndex, "pod"),
			Namespace:  safeGet(record, headerIndex, "namespace"),
			Value:      parseFloat(safeGet(record, headerIndex, "value")),
			LabelsRaw:  parseLabels(safeGet(record, headerIndex, "labels_raw")),
		}

		valueBytes, err := json.Marshal(t)
		if err != nil {
			log.Printf("error marshaling row %d: %v", rowCount, err)
			continue
		}

		msg := &pb.Message{
			Key:         []byte(strconv.Itoa(t.GPUId)),
			Value:       valueBytes,
			TimestampMs: t.Timestamp.UnixMilli(),
		}
		batch = append(batch, msg)

		if len(batch) >= batchSize {
			if err := sendBatch(client, topic, partition, batch); err != nil {
				log.Printf("failed to send batch: %v", err)
			}
			batch = batch[:0]
		}
		// break // todo: delete after test

	}

	// Flush leftover
	if len(batch) > 0 {
		if err := sendBatch(client, topic, partition, batch); err != nil {
			log.Printf("failed to send final batch: %v", err)
		}
	}

	log.Printf("processed %d rows from %s", rowCount, csvPath)
	return nil
}

func sendBatch(client pb.MQClient, topic string, partition int, msgs []*pb.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.ProduceRequest{
		Topic:     topic,
		Partition: int32(partition),
		Messages:  msgs,
		Acks:      pb.ProduceRequest_LEADER,
	}

	resp, err := client.Produce(ctx, req)
	if err != nil {
		return fmt.Errorf("Produce RPC error: %w", err)
	}

	log.Printf("batch produced: partition=%d, count=%d, lastOffset=%d",
		resp.Partition, len(resp.Offsets), resp.Offsets[len(resp.Offsets)-1])
	return nil
}

func safeGet(record []string, idx map[string]int, field string) string {
	if i, ok := idx[field]; ok && i < len(record) {
		return record[i]
	}
	return ""
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Now()
	}
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(ts)
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Now()
}

func parseInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func parseLabels(s string) map[string]string {
	if s == "" {
		return map[string]string{}
	}
	labels := make(map[string]string)
	pairs := strings.Split(s, ",")
	for _, p := range pairs {
		kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(kv) == 2 {
			labels[kv[0]] = kv[1]
		}
	}
	return labels
}
