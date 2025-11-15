package db

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/data-dispatcher-service/src/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// Client represents a PostgreSQL database client
type Client struct {
	pool   *pgxpool.Pool
	logger *logrus.Logger
}

// Batch represents a batch record from the database
type Batch struct {
	BatchID     string  `db:"batch_id"`
	SessionID   string  `db:"session_id"`
	BatchIndex  int     `db:"batch_index"`
	DataPayload []byte  `db:"data_payload"`
	Labels      []int32 `db:"labels"` // JSON array from database
	IsEnqueued  bool    `db:"is_enqueued"`
}

// NewClient creates a new database client with connection pooling
func NewClient(cfg config.Interface) (*Client, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	dbConfig := cfg.GetDatabaseConfig()

	// Build connection string for Cloud SQL Proxy
	// Format: postgresql://username:password@host:port/database
	connString := fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		dbConfig.GetUser(),
		dbConfig.GetPassword(),
		dbConfig.GetHost(),
		dbConfig.GetPort(),
		dbConfig.GetDBName(),
	)

	// Create connection pool
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info("Successfully connected to PostgreSQL database")

	return &Client{
		pool:   pool,
		logger: logger,
	}, nil
}

// GetPendingBatches retrieves all pending batches for a given session
func (c *Client) GetPendingBatches(ctx context.Context, sessionID string) ([]Batch, error) {
	query := `
		SELECT batch_id, session_id, batch_index, data_payload, labels, is_enqueued
		FROM batches
		WHERE session_id = $1 AND is_enqueued = false
		ORDER BY batch_index ASC
	`

	rows, err := c.pool.Query(ctx, query, sessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending batches: %w", err)
	}
	defer rows.Close()

	return c.scanBatches(rows)
}

// GetPendingBatchesLimit retrieves a limited number of pending batches for a given session
func (c *Client) GetPendingBatchesLimit(ctx context.Context, sessionID string, limit int) ([]Batch, error) {
	query := `
		SELECT batch_id, session_id, batch_index, data_payload, labels, is_enqueued
		FROM batches
		WHERE session_id = $1 AND is_enqueued = false
		ORDER BY batch_index ASC
		LIMIT $2
	`

	rows, err := c.pool.Query(ctx, query, sessionID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending batches with limit: %w", err)
	}
	defer rows.Close()

	return c.scanBatches(rows)
}

// scanBatches is a helper function to scan batch rows
func (c *Client) scanBatches(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) ([]Batch, error) {
	var batches []Batch
	for rows.Next() {
		var batch Batch
		var labelsJSON []byte

		err := rows.Scan(
			&batch.BatchID,
			&batch.SessionID,
			&batch.BatchIndex,
			&batch.DataPayload,
			&labelsJSON,
			&batch.IsEnqueued,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan batch row: %w", err)
		}

		// Parse JSON labels to []int32
		if len(labelsJSON) > 0 {
			if err := json.Unmarshal(labelsJSON, &batch.Labels); err != nil {
				return nil, fmt.Errorf("failed to unmarshal labels for batch %s: %w", batch.BatchID, err)
			}
		}

		batches = append(batches, batch)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating batch rows: %w", err)
	}

	return batches, nil
} // MarkBatchAsEnqueued updates a single batch status to enqueued
func (c *Client) MarkBatchAsEnqueued(ctx context.Context, batchID string) error {
	query := `
		UPDATE batches
		SET is_enqueued = true
		WHERE batch_id = $1
	`

	result, err := c.pool.Exec(ctx, query, batchID)
	if err != nil {
		return fmt.Errorf("failed to update batch status: %w", err)
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("batch not found: %s", batchID)
	}

	c.logger.WithField("batch_id", batchID).Debug("Marked batch as enqueued")
	return nil
}

// MarkBatchesAsEnqueued updates multiple batches status to enqueued in a single query
func (c *Client) MarkBatchesAsEnqueued(ctx context.Context, batchIDs []string) error {
	if len(batchIDs) == 0 {
		return nil // Nothing to update
	}

	query := `
		UPDATE batches
		SET is_enqueued = true
		WHERE batch_id = ANY($1)
	`

	result, err := c.pool.Exec(ctx, query, batchIDs)
	if err != nil {
		return fmt.Errorf("failed to update batches status: %w", err)
	}

	rowsAffected := result.RowsAffected()
	c.logger.WithFields(logrus.Fields{
		"batch_count":   len(batchIDs),
		"rows_affected": rowsAffected,
	}).Debug("Marked batches as enqueued")

	return nil
}

// Close closes the database connection pool
func (c *Client) Close() {
	c.pool.Close()
	c.logger.Info("Database connection pool closed")
}
