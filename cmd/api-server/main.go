package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	_ "github.com/jackc/pgx/v5/stdlib"
	httpSwagger "github.com/swaggo/http-swagger"

	_ "telemetry-pipeline/api/docs" // swagger docs
	"telemetry-pipeline/internal/handler"
	"telemetry-pipeline/internal/repository"
	"telemetry-pipeline/internal/service"
)

// @title Telemetry API
// @version 1.0
// @description API for querying GPU telemetry stored in TimescaleDB
// @BasePath /api/v1
func main() {
	// e.g. postgres://user:pass@localhost:5432/telemetry?sslmode=disable
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		os.Getenv("APP_DB_USER"),
		os.Getenv("APP_DB_PASSWORD"),
		os.Getenv("APP_DB_HOST"),
		os.Getenv("APP_DB_PORT"),
		os.Getenv("APP_DB_NAME"),
	)
	// log.Printf("DB datasource: %s", dsn)
	// DB connection
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
	r.HandleFunc("/api/v1/gpus/{gpu_id}/telemetry", h.GetGPUTelemetry).Methods("GET")
	r.HandleFunc("/api/v1/gpus/{gpu_id}/metrics", h.GetGPUMetrics).Methods("GET")
	r.PathPrefix("/swagger/").Handler(httpSwagger.WrapHandler)

	addr := ":8080"
	log.Printf("Starting API server on %s ...", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatal(err)
	}
}
