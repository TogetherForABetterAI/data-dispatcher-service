package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// Publisher handles RabbitMQ publishing operations
type Publisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	logger  *logrus.Logger
}

// Config holds RabbitMQ connection configuration
type Config struct {
	Host     string
	Port     int32
	Username string
	Password string
}

// BatchData represents the data structure to be published to RabbitMQ
type BatchData struct {
	ClientID     string      `json:"client_id"`
	BatchIndex   int32       `json:"batch_index"`
	Data         []byte      `json:"data"`
	IsLastBatch  bool        `json:"is_last_batch"`
	Timestamp    time.Time   `json:"timestamp"`
}

// NewPublisher creates a new RabbitMQ publisher with the given configuration
func NewPublisher(config Config) (*Publisher, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", 
		config.Username, config.Password, config.Host, config.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	publisher := &Publisher{
		conn:    conn,
		channel: channel,
		logger:  logger,
	}

	logger.WithFields(logrus.Fields{
		"host": config.Host,
		"port": config.Port,
		"user": config.Username,
	}).Info("Connected to RabbitMQ")

	return publisher, nil
}

// PublishBatch publishes a data batch to the specified queue
func (p *Publisher) PublishBatch(ctx context.Context, queueName string, batch *BatchData) error {
	// Marshal the batch data to JSON
	body, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch data: %w", err)
	}

	// Ensure the queue exists (passive=true means it won't create if it doesn't exist)
	_, err = p.channel.QueueDeclarePassive(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("queue %s does not exist or is not accessible: %w", queueName, err)
	}

	// Publish the message
	err = p.channel.PublishWithContext(
		ctx,
		"",        // exchange
		queueName, // routing key (queue name)
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			Timestamp:    batch.Timestamp,
			DeliveryMode: amqp.Persistent, // Make message persistent
			Headers: amqp.Table{
				"client_id":     batch.ClientID,
				"batch_index":   batch.BatchIndex,
				"is_last_batch": batch.IsLastBatch,
			},
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish message to queue %s: %w", queueName, err)
	}

	p.logger.WithFields(logrus.Fields{
		"queue_name":    queueName,
		"client_id":     batch.ClientID,
		"batch_index":   batch.BatchIndex,
		"is_last_batch": batch.IsLastBatch,
		"data_size":     len(batch.Data),
	}).Debug("Published batch to RabbitMQ")

	return nil
}

// PublishBatchWithRetry publishes a batch with retry logic
func (p *Publisher) PublishBatchWithRetry(ctx context.Context, queueName string, batch *BatchData, maxRetries int) error {
	var lastErr error
	
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(attempt) * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			
			p.logger.WithFields(logrus.Fields{
				"attempt":    attempt + 1,
				"max_retries": maxRetries + 1,
				"queue_name": queueName,
				"client_id":  batch.ClientID,
			}).Warn("Retrying to publish batch")
		}

		lastErr = p.PublishBatch(ctx, queueName, batch)
		if lastErr == nil {
			if attempt > 0 {
				p.logger.WithFields(logrus.Fields{
					"attempt":   attempt + 1,
					"queue_name": queueName,
					"client_id":  batch.ClientID,
				}).Info("Successfully published batch after retry")
			}
			return nil
		}

		p.logger.WithFields(logrus.Fields{
			"attempt":   attempt + 1,
			"error":     lastErr.Error(),
			"queue_name": queueName,
			"client_id":  batch.ClientID,
		}).Error("Failed to publish batch")
	}

	return fmt.Errorf("failed to publish after %d attempts: %w", maxRetries+1, lastErr)
}

// Close closes the RabbitMQ connection and channel
func (p *Publisher) Close() error {
	var channelErr, connErr error
	
	if p.channel != nil {
		channelErr = p.channel.Close()
	}
	
	if p.conn != nil {
		connErr = p.conn.Close()
	}

	if channelErr != nil {
		return fmt.Errorf("failed to close channel: %w", channelErr)
	}
	
	if connErr != nil {
		return fmt.Errorf("failed to close connection: %w", connErr)
	}

	p.logger.Info("Closed RabbitMQ connection")
	return nil
}

// IsConnected checks if the RabbitMQ connection is still alive
func (p *Publisher) IsConnected() bool {
	return p.conn != nil && !p.conn.IsClosed()
} 