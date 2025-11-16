package server

import (
	"context"
	"fmt"

	"github.com/data-dispatcher-service/src/db"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/models"
	datasetpb "github.com/data-dispatcher-service/src/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// BatchHandler manages fetching batches from DB and publishing to client queues
type BatchHandler struct {
	publisher *middleware.Publisher
	dbClient  DBClient
	logger    *logrus.Logger
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewBatchHandler creates a new batch handler with initialized dependencies
func NewBatchHandler(publisher *middleware.Publisher, dbClient DBClient, logger *logrus.Logger) *BatchHandler {
	ctx, cancel := context.WithCancel(context.Background())

	return &BatchHandler{
		publisher: publisher,
		dbClient:  dbClient,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start processes all batches for a client session in chunks
func (bh *BatchHandler) Start(notification *models.ConnectNotification, queueName string) error {
	bh.logger.WithFields(logrus.Fields{
		"client_id":  notification.ClientId,
		"session_id": notification.SessionId,
		"queue_name": queueName,
	}).Info("Starting batch handler for client session")

	const batchChunkSize = 5 // Number of batches to process per iteration
	totalProcessed := 0

	// Loop until no more pending batches
	for {
		select {
		case <-bh.ctx.Done():
			return bh.ctx.Err() // Context cancelled due to shutdown signal
		default:
			// Continue processing
		}

		// Get N+1 batches to detect if this is the last chunk
		// Strategy: Request one extra batch to "look ahead"
		batches, err := bh.dbClient.GetPendingBatchesLimit(bh.ctx, notification.SessionId, batchChunkSize+1)
		if err != nil {
			return fmt.Errorf("failed to get pending batches: %w", err)
		}

		// If no more batches, we're done
		if len(batches) == 0 {
			bh.logger.WithFields(logrus.Fields{
				"client_id":       notification.ClientId,
				"session_id":      notification.SessionId,
				"total_processed": totalProcessed,
			}).Info("All batches processed for client session")
			break
		}

		// Determine if this is the last chunk
		// If we received <= batchChunkSize batches, it means there are no more after this
		isLastChunk := len(batches) <= batchChunkSize

		// Prepare the chunk to process
		var chunkToProcess []db.Batch
		if isLastChunk {
			// This is the last chunk, process all received batches
			chunkToProcess = batches
		} else {
			// We received N+1 batches, so process only the first N
			chunkToProcess = batches[:batchChunkSize]
		}

		bh.logger.WithFields(logrus.Fields{
			"session_id":    notification.SessionId,
			"chunk_size":    len(chunkToProcess),
			"is_last_chunk": isLastChunk,
			"chunk_number":  (totalProcessed / batchChunkSize) + 1,
		}).Debug("Retrieved chunk of pending batches")

		// Process this chunk
		if err := bh.processBatchChunk(chunkToProcess, notification.ClientId, notification.SessionId, queueName, isLastChunk); err != nil {
			return fmt.Errorf("failed to process batch chunk: %w", err)
		}

		totalProcessed += len(chunkToProcess)

		// If this was the last chunk, we're done
		if isLastChunk {
			break
		}
	}

	return nil
}

// processBatchChunk handles publishing and marking a chunk of batches
func (bh *BatchHandler) processBatchChunk(batches []db.Batch, clientID, sessionID, queueName string, isLastChunk bool) error {
	batchIDs := make([]string, 0, len(batches))

	// Publish all batches in the chunk
	for i, batch := range batches {
		select {
		case <-bh.ctx.Done():
			return bh.ctx.Err()
		default:
			// Continue processing
		}

		// Determine if this is the last batch of the entire session
		// It's the last batch ONLY if:
		// 1. This is the last chunk (isLastChunk == true) AND
		// 2. This is the last item in this chunk (i == len(batches) - 1)
		isLastBatch := isLastChunk && (i == len(batches)-1)

		// Publish the batch
		if err := bh.PublishBatch(batch, clientID, sessionID, queueName, isLastBatch); err != nil {
			// If publish fails, don't mark any batch as enqueued
			return fmt.Errorf("failed to publish batch %s (index %d in chunk): %w", batch.BatchID, i, err)
		}

		batchIDs = append(batchIDs, batch.BatchID)

		bh.logger.WithFields(logrus.Fields{
			"client_id":     clientID,
			"session_id":    sessionID,
			"batch_id":      batch.BatchID,
			"batch_index":   batch.BatchIndex,
			"is_last_batch": isLastBatch,
		}).Debug("Batch published successfully")
	}

	// Mark all batches in this chunk as enqueued in a single DB operation
	if err := bh.dbClient.MarkBatchesAsEnqueued(bh.ctx, batchIDs); err != nil {
		bh.logger.WithError(err).WithFields(logrus.Fields{
			"batch_count": len(batchIDs),
			"batch_ids":   batchIDs,
		}).Error("Failed to mark batches as enqueued, but messages were published. Idempotency will handle duplicates.")
		// Don't return error - messages already published, idempotency will handle it
	}

	bh.logger.WithFields(logrus.Fields{
		"client_id":     clientID,
		"session_id":    sessionID,
		"batch_count":   len(batches),
		"is_last_chunk": isLastChunk,
	}).Info("Successfully processed batch chunk")

	return nil
}

// PublishBatch handles the transformation and publishing of a single batch
func (bh *BatchHandler) PublishBatch(batch db.Batch, clientID, sessionID, queueName string, isLastBatch bool) error {
	// Create protobuf message
	batchMsg := &datasetpb.DataBatchLabeled{
		Data:        batch.DataPayload,
		Labels:      batch.Labels,
		BatchIndex:  int32(batch.BatchIndex),
		IsLastBatch: isLastBatch,
	}

	// Marshal to protobuf
	msgBody, err := proto.Marshal(batchMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal batch to protobuf: %w", err)
	}

	// Publish to client's queue using default exchange (direct queue publish)
	// Routing key = queue name, exchange = "" (default exchange)
	if err := bh.publisher.Publish(queueName, msgBody, ""); err != nil {
		return fmt.Errorf("failed to publish to queue %s: %w", queueName, err)
	}

	bh.logger.WithFields(logrus.Fields{
		"client_id":    clientID,
		"session_id":   sessionID,
		"batch_index":  batch.BatchIndex,
		"queue_name":   queueName,
		"data_size":    len(batch.DataPayload),
		"labels_count": len(batch.Labels),
	}).Debug("Batch published successfully")

	return nil
}

// Stop cancels the batch handler's context
func (bh *BatchHandler) Stop() {
	bh.cancel()
	bh.logger.Info("BatchHandler stopped")
}
