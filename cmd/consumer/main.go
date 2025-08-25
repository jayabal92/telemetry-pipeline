package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"

	"telemetry-pipeline/internal/model"
	pb "telemetry-pipeline/proto"
)

const (
	maxMessages = 1000
	waitMs      = 500
)

func main() {
	// === Load Config from ENV ===
	queueAddr := getEnv("MSG_QUEUE_ADDR", "127.0.0.1:9092")
	topic := getEnv("MSG_QUEUE_TOPIC", "events")
	partition := int32(-1)
	// groupID := getEnv("CONSUMER_GROUP", "telemetry-consumers")

	dbHost := getEnv("APP_DB_HOST", "localhost")
	dbPort := getEnv("APP_DB_PORT", "5432")
	dbUser := getEnv("APP_DB_USER", "telemetry")
	dbPassword := getEnv("APP_DB_PASSWORD", "telemetry123")
	dbName := getEnv("APP_DB_NAME", "telemetry")

	// === Connect to DB ===
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("❌ failed to connect to Postgres: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("❌ DB unreachable: %v", err)
	}
	log.Println("✅ Connected to PostgreSQL")

	// === Connect to gRPC MQ ===
	conn, err := grpc.NewClient(queueAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("❌ failed to dial msg-queue: %v", err)
	}
	defer conn.Close()

	client := pb.NewMQClient(conn)

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleSignals(cancel)

	offset := int64(0) // TODO: in real impl, fetch committed offset from DB or msg-queue

	for {
		select {
		case <-ctx.Done():
			log.Println("⏹ Consumer shutting down...")
			return
		default:
			// === Fetch from MQ ===
			log.Printf("Sending Fetch request: topic: %s partition: %d offset: %d Maxmsgs: %d", topic, partition, offset, maxMessages)
			resp, err := client.Fetch(ctx, &pb.FetchRequest{
				Topic:       topic,
				Partition:   partition,
				Offset:      offset,
				MaxMessages: maxMessages,
				WaitMs:      waitMs,
			})
			if err != nil {
				log.Printf("⚠️ fetch error: %v", err)
				time.Sleep(time.Second)
				continue
			}

			if len(resp.Records) == 0 {
				log.Println("no records received")
				time.Sleep(2 * time.Second)
				continue
			}

			log.Printf("number of messages received %d", len(resp.Records))
			// === Insert records into DB ===
			for _, rec := range resp.Records {
				log.Printf("📥 Received offset=%d key=%s value=%s headers=%v",
					rec.Offset, string(rec.Message.Key), string(rec.Message.Value), rec.Message.Headers)
				offset = rec.Offset + 1
				var telemetry model.Telemetry
				if err := json.Unmarshal(rec.Message.Value, &telemetry); err != nil {
					log.Printf("⚠️ json unmarshal failed: %v", err)
					continue
				}

				_, err = db.ExecContext(ctx, `
				INSERT INTO gpu_telemetry (
					ts, metric_name, gpu_id, device, uuid, model_name,
					hostname, container, pod, namespace, metric_value, labels_raw
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			`,
					telemetry.Timestamp,
					telemetry.MetricName,
					telemetry.GPUId,
					telemetry.Device,
					telemetry.UUID,
					telemetry.ModelName,
					telemetry.Hostname,
					telemetry.Container,
					telemetry.Pod,
					telemetry.Namespace,
					telemetry.Value,
					toJSONB(telemetry.LabelsRaw),
				)
				if err != nil {
					log.Printf("❌ failed inserting telemetry row: %v", err)
				} else {
					log.Printf("✅ Inserted record with offset %d", rec.Offset)
				}
				offset = rec.Offset + 1
			}
		}
	}
}

func toJSONB(m map[string]string) []byte {
	if m == nil {
		return []byte(`{}`)
	}
	b, _ := json.Marshal(m)
	return b
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func handleSignals(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	cancel()
}
