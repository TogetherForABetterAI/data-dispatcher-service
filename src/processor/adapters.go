package processor

import (
	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/sirupsen/logrus"
)

// ------ Logrus adapter ------
type LogrusAdapter struct {
	logger *logrus.Logger
}

func (l *LogrusAdapter) WithFields(fields map[string]interface{}) Logger {
	return &LogrusEntryAdapter{entry: l.logger.WithFields(fields)}
}

func (l *LogrusAdapter) Info(args ...interface{}) {
	l.logger.Info(args...)
}

func (l *LogrusAdapter) Error(args ...interface{}) {
	l.logger.Error(args...)
}

type LogrusEntryAdapter struct {
	entry *logrus.Entry
}

func (l *LogrusEntryAdapter) WithFields(fields map[string]interface{}) Logger {
	return &LogrusEntryAdapter{entry: l.entry.WithFields(fields)}
}

func (l *LogrusEntryAdapter) Info(args ...interface{}) {
	l.entry.Info(args...)
}

func (l *LogrusEntryAdapter) Error(args ...interface{}) {
	l.entry.Error(args...)
}

// ------ GlobalConfig adapter ------
type GlobalConfigAdapter struct {
	config *config.GlobalConfig
}

func NewGlobalConfigAdapter(config *config.GlobalConfig) *GlobalConfigAdapter {
	return &GlobalConfigAdapter{config: config}
}

func (g *GlobalConfigAdapter) GetDatasetServiceAddr() string {
	return g.config.GrpcConfig.DatasetAddr
}

func (g *GlobalConfigAdapter) GetMaxRetries() int {
	return g.config.MiddlewareConfig.MaxRetries
}

func (g *GlobalConfigAdapter) GetDatasetName() string {
	return g.config.GrpcConfig.DatasetName
}

func (g *GlobalConfigAdapter) GetBatchSize() int32 {
	return g.config.GrpcConfig.BatchSize
}

func (g *GlobalConfigAdapter) GetRabbitConfig() *config.MiddlewareConfig {
	return g.config.MiddlewareConfig
}
