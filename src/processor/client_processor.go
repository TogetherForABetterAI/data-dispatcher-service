package processor

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/mlops-eval/data-dispatcher-service/src/grpc"
	"github.com/mlops-eval/data-dispatcher-service/src/pb"
	"github.com/mlops-eval/data-dispatcher-service/src/rabbitmq"
	"github.com/sirupsen/logrus"
)

// ClientDataProcessor handles processing client data requests
type ClientDataProcessor struct {
	datasetServiceAddr string
	logger             *logrus.Logger
	maxRetries         int
}

// NewClientDataProcessor creates a new client data processor
func NewClientDataProcessor() *ClientDataProcessor {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	// Get dataset service address from environment or use default
	datasetAddr := os.Getenv("DATASET_SERVICE_ADDR")
	if datasetAddr == "" {
		datasetAddr = "localhost:50051" // default from existing code
	}

	// Get max retries from environment or use default
	maxRetries := 3
	if retriesStr := os.Getenv("MAX_RETRIES"); retriesStr != "" {
		if parsed, err := strconv.Atoi(retriesStr); err == nil {
			maxRetries = parsed
		}
	}

	return &ClientDataProcessor{
		datasetServiceAddr: datasetAddr,
		logger:             logger,
		maxRetries:         maxRetries,
	}
}

// ProcessClient implements the ClientProcessor interface
func (p *ClientDataProcessor) ProcessClient(ctx context.Context, req *pb.NewClientRequest) error {
	p.logger.WithFields(logrus.Fields{
		"client_id":  req.ClientId,
		"queue_name": req.QueueName,
	}).Info("Starting client data processing")

	// Create RabbitMQ publisher
	rabbitConfig := rabbitmq.Config{
		Host:     req.AmqpHost,
		Port:     req.AmqpPort,
		Username: req.AmqpUser,
		Password: req.AmqpPass,
	}

	publisher, err := rabbitmq.NewPublisher(rabbitConfig)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ publisher: %w", err)
	}
	defer publisher.Close()

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
		
		batchReq := &pb.GetBatchRequest{
			DatasetName: datasetName,
			BatchSize:   batchSize,
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

		// Publish to RabbitMQ with retry
		if err := publisher.PublishBatchWithRetry(ctx, req.QueueName, rabbitBatch, p.maxRetries); err != nil {
			p.logger.WithFields(logrus.Fields{
				"client_id":   req.ClientId,
				"batch_index": batchIndex,
				"queue_name":  req.QueueName,
				"error":       err.Error(),
			}).Error("Failed to publish batch to RabbitMQ")
			return fmt.Errorf("failed to publish batch %d to queue %s: %w", batchIndex, req.QueueName, err)
		}

		p.logger.WithFields(logrus.Fields{
			"client_id":     req.ClientId,
			"batch_index":   batchIndex,
			"queue_name":    req.QueueName,
			"is_last_batch": batch.GetIsLastBatch(),
			"data_size":     len(batch.GetData()),
		}).Info("Successfully published batch")

		// Check if this was the last batch
		if batch.GetIsLastBatch() {
			p.logger.WithFields(logrus.Fields{
				"client_id":      req.ClientId,
				"total_batches":  batchIndex + 1,
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