package log

import (
	"sync"

	"go.uber.org/zap"
)

var (
	logger *zap.Logger
	once   sync.Once
)

// GetLogger returns a singleton zap.Logger instance
func GetLogger() *zap.Logger {
	once.Do(func() {
		cfg := zap.NewProductionConfig()
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		cfg.Encoding = "json"
		cfg.EncoderConfig.TimeKey = "ts"
		cfg.EncoderConfig.LevelKey = "level"
		cfg.EncoderConfig.MessageKey = "msg"
		l, err := cfg.Build()
		if err != nil {
			panic(err)
		}
		logger = l
	})
	return logger
}
