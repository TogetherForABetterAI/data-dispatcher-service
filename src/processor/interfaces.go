package processor

import (
	"context"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
	datasetpb "github.com/mlops-eval/data-dispatcher-service/src/pb/dataset-service"
)

type DatasetServiceClient interface {
	GetBatch(ctx context.Context, req *datasetpb.GetBatchRequest) (*datasetpb.DataBatchLabeled, error)
	Close() error
}

type IMiddleware interface {
	Publish(routingKey string, message []byte, exchangeName string) error
	Close()
}

type Logger interface {
	WithFields(fields map[string]interface{}) Logger
	Info(args ...interface{})
	Error(args ...interface{})
}

type ConfigProvider interface {
	GetDatasetServiceAddr() string
	GetMaxRetries() int
	GetDatasetName() string
	GetBatchSize() int32
	GetRabbitConfig() *config.MiddlewareConfig
}
