package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"telemetry-pipeline/internal/broker"
	"telemetry-pipeline/internal/config"
	"telemetry-pipeline/internal/log"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// BrokerServer abstracts the broker.Server for easier testing
type BrokerServer interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

func run(ctx context.Context, cfg config.Config, logger *zap.Logger, etcd *clientv3.Client, srv BrokerServer) error {
	logger.Info("creating broker server:", zap.Any("nodeId", cfg.Server.NodeID))

	logger.Info("Starting server:", zap.Any("nodeId", cfg.Server.NodeID))
	if err := srv.Start(ctx); err != nil {
		return err
	}
	logger.Info("broker running:", zap.Any("nodeId", cfg.Server.NodeID))

	// wait for shutdown signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("shutting down broker...")
	if err := srv.Shutdown(context.Background()); err != nil {
		logger.Error("shutdown failed:", zap.Any("error", err))
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

func main() {
	cfgFile := flag.String("config", "./configs/config.yaml", "config file")
	flag.Parse()

	// structured logger (zap)
	logger := log.GetLogger()
	defer logger.Sync()

	logger.Info("loading config from", zap.Any("configFile", *cfgFile))
	b, err := os.ReadFile(*cfgFile)
	if err != nil {
		logger.Fatal("cannot read config:", zap.Any("error", err))
	}

	var cfg config.Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		logger.Fatal("invalid config yaml:", zap.Any("error", err))
	}
	cfg.Server.NodeID = expandPlaceholders(cfg.Server.NodeID)
	cfg.Server.GRPCAdvertisedAddr = expandPlaceholders(cfg.Server.GRPCAdvertisedAddr)

	logger.Info("config loaded:", zap.Any("config", cfg.String()))

	logger.Info("connecting to etcd endpoints:", zap.Any("etcd endpoints", cfg.Etcd.Endpoints))
	etc, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		Username:    cfg.Etcd.Username,
		Password:    cfg.Etcd.Password,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Fatal("etcd connection failed:", zap.Any("error", err))
	}
	defer etc.Close()
	logger.Info("connected to etcd")

	// init broker
	s := broker.NewServer(cfg, logger, etc)

	// run broker logic
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx, cfg, logger, etc, s); err != nil {
		logger.Fatal("server error", zap.Error(err))
	}
}

func expandPlaceholders(val string) string {
	if strings.Contains(val, "$(POD_NAME)") {
		podName := os.Getenv("POD_NAME")
		return strings.ReplaceAll(val, "$(POD_NAME)", podName)
	}
	return val
}
