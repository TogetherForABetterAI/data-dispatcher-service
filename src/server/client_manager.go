package server

import (
	"fmt"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/db"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/models"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// ClientManager handles processing client data requests
type ClientManager struct {
	clientID     string
	logger       *logrus.Logger
	conn         *amqp.Connection
	middleware   middleware.MiddlewareInterface
	dbClient     *db.Client
	batchHandler *BatchHandler
}

type ClientManagerInterface interface {
	HandleClient(notification *models.ConnectNotification) error
	Stop()
}

// NewClientManager creates a new client manager
func NewClientManager(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) *ClientManager {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	// Create database client
	dbClient, err := db.NewClient(cfg)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create database client")
	}

	return &ClientManager{
		logger:     logger,
		conn:       mw.Conn(),
		middleware: mw,
		clientID:   clientID,
		dbClient:   dbClient,
	}
}

// HandleClient processes a client notification by fetching batches from DB and publishing to client queue
func (c *ClientManager) HandleClient(notification *models.ConnectNotification) error {
	c.logger.WithFields(logrus.Fields{
		"client_id":  notification.ClientId,
		"session_id": notification.SessionId,
	}).Info("Starting to handle client notification")

	// Create RabbitMQ publisher using shared connection
	publisher, err := middleware.NewPublisher(c.conn)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ publisher: %w", err)
	}
	defer publisher.Close()

	// Create and declare client's dispatcher queue
	queueName := fmt.Sprintf("%s_dispatcher_queue", notification.ClientId)
	if err := c.middleware.DeclareQueue(queueName); err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", queueName, err)
	}

	// Create batch handler
	c.batchHandler = NewBatchHandler(publisher, c.dbClient, c.logger)

	// Start processing batches
	return c.batchHandler.Start(notification, queueName)
}

func (c *ClientManager) Stop() {
	c.logger.Info("Stopping ClientManager for client ", c.clientID)
	if c.batchHandler != nil {
		c.batchHandler.Stop()
	}
	if c.dbClient != nil {
		c.dbClient.Close()
	}
}
