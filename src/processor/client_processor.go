package processor

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/mlops-eval/data-dispatcher-service/src/grpc"
	datasetpb "github.com/mlops-eval/data-dispatcher-service/src/pb/dataset-service"
	clientpb "github.com/mlops-eval/data-dispatcher-service/src/pb/new-client-service"
	"github.com/mlops-eval/data-dispatcher-service/src/utils/config"
	"github.com/sirupsen/logrus"
)

// ClientDataProcessor handles processing client data requests
type ClientDataProcessor struct {
	datasetServiceAddr string
	logger             *logrus.Logger
	maxRetries         int
	rabbitConfig       config.MiddlewareConfig
}

// NewClientDataProcessor creates a new client data processor
func NewClientDataProcessor() *ClientDataProcessor {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	// Get dataset service address from environment or use default
	datasetAddr := os.Getenv("DATASET_SERVICE_ADDR")
	if datasetAddr == "" {
		datasetAddr = "dataset-grpc-service:50051" // default from existing code
	}

	// Get max retries from environment or use default
	maxRetries := 3
	if retriesStr := os.Getenv("MAX_RETRIES"); retriesStr != "" {
		if parsed, err := strconv.Atoi(retriesStr); err == nil {
			maxRetries = parsed
		}
	}

	// Get RabbitMQ connection details from environment
	rabbitHost := os.Getenv("RABBITMQ_HOST")
	if rabbitHost == "" {
		rabbitHost = "localhost"
	}

	rabbitPort := int32(5672) // default RabbitMQ port
	if portStr := os.Getenv("RABBITMQ_PORT"); portStr != "" {
		if parsed, err := strconv.ParseInt(portStr, 10, 32); err == nil {
			rabbitPort = int32(parsed)
		}
	}

	rabbitUser := os.Getenv("RABBITMQ_USER")
	if rabbitUser == "" {
		rabbitUser = "guest"
	}

	rabbitPass := os.Getenv("RABBITMQ_PASS")
	if rabbitPass == "" {
		rabbitPass = "guest"
	}

	rabbitConfig := config.MiddlewareConfig{
		Host:     rabbitHost,
		Port:     rabbitPort,
		Username: rabbitUser,
		Password: rabbitPass,
	}

	return &ClientDataProcessor{
		datasetServiceAddr: datasetAddr,
		logger:             logger,
		maxRetries:         maxRetries,
		rabbitConfig:       rabbitConfig,
	}
}

// ProcessClient implements the ClientProcessor interface
func (p *ClientDataProcessor) ProcessClient(ctx context.Context, req *clientpb.NewClientRequest) error {
	p.logger.WithFields(logrus.Fields{
		"client_id":   req.ClientId,
		"routing_key": req.RoutingKey,
	}).Info("Starting client data processing")

	// Create RabbitMQ middleware
	middleware, err := rabbitmq.NewMiddleware(p.rabbitConfig)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ middleware: %w", err)
	}
	defer middleware.Close()

	// Create gRPC client for dataset service
	grpcClient, err := grpc.NewClient(p.datasetServiceAddr)
	if err != nil {
		return fmt.Errorf("failed to create dataset service client: %w", err)
	}

	// Get dataset configuration from environment or use defaults
	datasetName := os.Getenv("DATASET_NAME")
	if datasetName == "" {
		datasetName = "mnist" // default from existing code
	}

	batchSizeStr := os.Getenv("BATCH_SIZE")
	batchSize := int32(30) // default from existing code
	if batchSizeStr != "" {
		if parsed, err := strconv.ParseInt(batchSizeStr, 10, 32); err == nil {
			batchSize = int32(parsed)
		}
	}

	// Start processing batches
	batchIndex := int32(0)
	for {
		select {
		case <-ctx.Done():
			p.logger.WithField("client_id", req.ClientId).Info("Client processing cancelled")
			return ctx.Err()
		default:
		}

		// Fetch batch from dataset service with timeout
		batchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)

		batchReq := &datasetpb.GetBatchRequest{
			DatasetName: datasetName,
			BatchSize:   BATHSIZE,
			BatchIndex:  batchIndex,
		}

		batch, err := grpcClient.GetBatch(batchCtx, batchReq)
		cancel()

		if err != nil {
			p.logger.WithFields(logrus.Fields{
				"client_id":   req.ClientId,
				"batch_index": batchIndex,
				"error":       err.Error(),
			}).Error("Failed to fetch batch from dataset service")
			return fmt.Errorf("failed to fetch batch %d: %w", batchIndex, err)
		}

		// Prepare RabbitMQ message
		rabbitBatch := &rabbitmq.BatchData{
			ClientID:    req.ClientId,
			BatchIndex:  batchIndex,
			Data:        batch.GetData(),
			IsLastBatch: batch.GetIsLastBatch(),
			Timestamp:   time.Now(),
		}

		// Publish to both exchanges using routing key with retry
		exchanges := []string{"dataset-exchange", "calibration-exchange"}
		for _, exchange := range exchanges {
			if err := middleware.Publish(ctx, req.RoutingKey, rabbitBatch, exchange); err != nil {
				p.logger.WithFields(logrus.Fields{
					"client_id":   req.ClientId,
					"batch_index": batchIndex,
					"routing_key": req.RoutingKey,
					"error":       err.Error(),
				}).Error("Failed to publish batch to exchanges")
				return fmt.Errorf("failed to publish batch %d with routing key %s: %w", batchIndex, req.RoutingKey, err)
			}
		}

		p.logger.WithFields(logrus.Fields{
			"client_id":     req.ClientId,
			"batch_index":   batchIndex,
			"routing_key":   req.RoutingKey,
			"is_last_batch": batch.GetIsLastBatch(),
			"data_size":     len(batch.GetData()),
		}).Info("Successfully published batch to both exchanges")

		// Check if this was the last batch
		if batch.GetIsLastBatch() {
			p.logger.WithFields(logrus.Fields{
				"client_id":     req.ClientId,
				"total_batches": batchIndex + 1,
			}).Info("Completed data processing for client")
			break
		}

		batchIndex++

		// Add small delay between batches to avoid overwhelming the services
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	return nil
}
