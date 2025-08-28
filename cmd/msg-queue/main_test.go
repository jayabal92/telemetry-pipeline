package main

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"telemetry-pipeline/internal/config"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Mock BrokerServer ----
type mockServer struct {
	startErr    error
	shutdownErr error
	startCalled bool
	stopCalled  bool
}

func (m *mockServer) Start(ctx context.Context) error {
	m.startCalled = true
	return m.startErr
}

func (m *mockServer) Shutdown(ctx context.Context) error {
	m.stopCalled = true
	return m.shutdownErr
}

// ---- Helper: Test logger (zap in-memory) ----
func newTestLogger() *zap.Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	l, _ := cfg.Build()
	return l
}

// ---- Tests ----

func TestExpandPlaceholders_Positive(t *testing.T) {
	os.Setenv("POD_NAME", "pod-123")
	input := "node-$(POD_NAME)"
	got := expandPlaceholders(input)
	assert.Equal(t, "node-pod-123", got)
}

func TestExpandPlaceholders_NoPlaceholder(t *testing.T) {
	input := "node-xyz"
	got := expandPlaceholders(input)
	assert.Equal(t, "node-xyz", got)
}

func TestRun_Positive(t *testing.T) {
	mc := &mockServer{}
	logger := newTestLogger()
	cfg := config.Config{}
	etcd := &clientv3.Client{} // dummy, not used

	// Send SIGTERM after short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		p, _ := os.FindProcess(os.Getpid())
		_ = p.Signal(syscall.SIGTERM)
	}()

	err := run(context.Background(), cfg, logger, etcd, mc)
	require.NoError(t, err)
	assert.True(t, mc.startCalled)
	assert.True(t, mc.stopCalled)
}

func TestRun_StartError(t *testing.T) {
	mc := &mockServer{startErr: errors.New("boom")}
	logger := newTestLogger()
	cfg := config.Config{}
	etcd := &clientv3.Client{}

	err := run(context.Background(), cfg, logger, etcd, mc)
	assert.EqualError(t, err, "boom")
	assert.True(t, mc.startCalled)
	assert.False(t, mc.stopCalled)
}

func TestRun_ShutdownError(t *testing.T) {
	mc := &mockServer{shutdownErr: errors.New("fail shutdown")}
	logger := newTestLogger()
	cfg := config.Config{}
	etcd := &clientv3.Client{}

	// Send shutdown signal
	go func() {
		time.Sleep(100 * time.Millisecond)
		p, _ := os.FindProcess(os.Getpid())
		_ = p.Signal(syscall.SIGTERM)
	}()

	err := run(context.Background(), cfg, logger, etcd, mc)
	assert.EqualError(t, err, "fail shutdown")
	assert.True(t, mc.startCalled)
	assert.True(t, mc.stopCalled)
}
