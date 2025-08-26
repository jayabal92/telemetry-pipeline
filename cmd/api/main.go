package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	_ "github.com/jackc/pgx/v5/stdlib"

	"telemetry-pipeline/internal/handler"
	"telemetry-pipeline/internal/repository"
	"telemetry-pipeline/internal/service"
)

func main() {
	// DB connection
	dsn := os.Getenv("DB_DSN") // e.g. postgres://user:pass@localhost:5432/telemetry?sslmode=disable
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("DB connection failed: %v", err)
	}
	defer db.Close()

	// Router
	r := mux.NewRouter()

	// init repo + service + handler
	repo := repository.NewGPURepository(db)
	svc := service.NewGPUService(repo)
	h := handler.NewGPUHandler(svc)

	// Register handlers
	r.HandleFunc("/api/v1/gpus", h.ListGPUs).Methods("GET")
	r.HandleFunc("/api/v1/gpus/{id:[0-9]+}/telemetry", h.GetGPUTelemetry).Methods("GET")
	r.HandleFunc("/api/v1/gpus/{id:[0-9]+}/metrics/{metric_name}", h.GetGPUMetrics).Methods("GET")

	addr := ":8080"
	log.Printf("Starting API server on %s ...", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatal(err)
	}
}
