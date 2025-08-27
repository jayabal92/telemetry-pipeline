package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"

	"telemetry-pipeline/internal/model"
	pb "telemetry-pipeline/proto"
)

const (
	batchSize   = 500
	maxMessages = 1000
	waitMs      = 500
)

func main() {
	// === Load Config from ENV ===
	queueAddr := getEnv("MQ_SERVER", "127.0.0.1:9092")
	topic := getEnv("MQ_TOPIC", "events")

	partitionTemp, err := strconv.ParseInt(getEnv("MQ_PARTITION", "0"), 10, 32)
	if err != nil {
		log.Fatalf("failed to parse partition value: %v", err)
	}
	partition := int32(partitionTemp)
	groupID := getEnv("CONSUMER_GROUP", "telemetry-consumers")

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
		log.Fatalf("failed to connect to Postgres: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("DB unreachable: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	// === Connect to DB (pgx) ===
	conn, err := pgx.Connect(context.Background(), psqlInfo)
	if err != nil {
		log.Fatalf("failed to connect to Postgres: %v", err)
	}
	defer conn.Close(context.Background())
	log.Println("Connected to PostgreSQL via pgx")

	// === Connect to gRPC MQ ===
	grpcConn, err := grpc.NewClient(queueAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial msg-queue: %v", err)
	}
	defer grpcConn.Close()

	client := pb.NewMQClient(grpcConn)

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleSignals(cancel)

	// Get last committed offset
	offset := loadOffset(ctx, conn, groupID, topic, int(partition))

	var batch []model.Telemetry

	for {
		select {
		case <-ctx.Done():
			log.Println("⏹ Consumer shutting down...")
			// flush leftover
			if len(batch) > 0 {
				_ = copyInsert(ctx, conn, batch)
			}
			return
		default:
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
				time.Sleep(500 * time.Millisecond)
				continue
			}

			for _, rec := range resp.Records {
				offset = rec.Offset + 1

				var telemetry model.Telemetry
				if err := json.Unmarshal(rec.Message.Value, &telemetry); err != nil {
					log.Printf("json unmarshal failed: %v", err)
					continue
				}
				enrichGPUTelemetry(&telemetry)

				batch = append(batch, telemetry)

				if len(batch) >= batchSize {
					if err := copyInsert(ctx, conn, batch); err != nil {
						log.Printf("bulk insert failed: %v", err)
					} else {
						log.Printf("Inserted %d rows (last offset=%d)", len(batch), rec.Offset)
						commitOffset(ctx, conn, groupID, topic, int(partition), offset)
					}
					batch = batch[:0]
				}
			}

			// insert if any thing left off
			log.Printf("batch size left over: %v", len(batch))
			if len(batch) > 0 {
				if err := copyInsert(ctx, conn, batch); err != nil {
					log.Printf("bulk insert failed: %v", err)
				} else {
					log.Printf("Inserted remaining %d rows (last offset=%d)", len(batch), offset)
					commitOffset(ctx, conn, groupID, topic, int(partition), offset)
				}
				batch = batch[:0]
			}
		}
	}
}

// copyInsert uses pgx.CopyFrom for bulk insert
func copyInsert(ctx context.Context, conn *pgx.Conn, telemetryBatch []model.Telemetry) error {
	rows := make([][]interface{}, len(telemetryBatch))
	for i, t := range telemetryBatch {
		rows[i] = []interface{}{
			t.Timestamp,
			t.MetricName,
			t.GPUId,
			t.Device,
			t.UUID,
			t.ModelName,
			t.Hostname,
			t.Container,
			t.Pod,
			t.Namespace,
			t.Value,
			toJSONB(t.LabelsRaw),
		}
	}

	_, err := conn.CopyFrom(
		ctx,
		pgx.Identifier{"gpu_telemetry"},
		[]string{
			"ts", "metric_name", "gpu_id", "device", "uuid", "model_name",
			"hostname", "container", "pod", "namespace", "metric_value", "labels_raw",
		},
		pgx.CopyFromRows(rows),
	)
	return err
}

// persist last committed offset
func commitOffset(ctx context.Context, conn *pgx.Conn, groupID, topic string, partition int, offset int64) {
	_, err := conn.Exec(ctx, `
		INSERT INTO consumer_offsets (group_id, topic, partition, committed_offset)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (group_id, topic, partition)
		DO UPDATE SET committed_offset = EXCLUDED.committed_offset, updated_at = now()`,
		groupID, topic, partition, offset)
	if err != nil {
		log.Printf("⚠️ failed to commit offset: %v", err)
	}
}

// load last committed offset
func loadOffset(ctx context.Context, conn *pgx.Conn, groupID, topic string, partition int) int64 {
	var offset int64
	err := conn.QueryRow(ctx, `
		SELECT committed_offset FROM consumer_offsets 
		WHERE group_id=$1 AND topic=$2 AND partition=$3`,
		groupID, topic, partition).Scan(&offset)
	if err != nil {
		log.Printf("no previous offset, starting at 0: %v", err)
		return 0
	}
	log.Printf("Resuming from committed offset %d", offset)
	return offset
}

func enrichGPUTelemetry(telemetry *model.Telemetry) {
	telemetry.Timestamp = time.Now().UTC()
	// log.Printf("GPU Telemetry after enrich: %v", telemetry)
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
