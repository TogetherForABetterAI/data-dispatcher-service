package server

import (
	"fmt"
	"sync"

	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/mlops-eval/data-dispatcher-service/src/middleware"
	"github.com/sirupsen/logrus"
)

// Server handles RabbitMQ server operations for client notifications
type Server struct {
	middleware *middleware.Middleware
	logger     *logrus.Logger
	listener   *Listener
	clientWg   sync.WaitGroup // Track active client goroutines
	shutdown   chan struct{}  // Signal for graceful shutdown
	config     config.GlobalConfig
}

// NewServer creates a new RabbitMQ server for client notifications
func NewServer(cfg config.GlobalConfig) (*Server, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	// Use middleware to establish RabbitMQ connection
	middleware, err := middleware.NewMiddleware(cfg.GetMiddlewareConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware: %w", err)
	}

	// Create client manager with shared connection
	clientManager := NewClientManager(cfg, middleware.Conn(), middleware)

	// Create listener (queueName is now set in constructor)
	listener := NewListener(clientManager, middleware, logger)

	server := &Server{
		middleware: middleware,
		logger:     logger,
		listener:   listener,
		shutdown:   make(chan struct{}),
		config:     cfg,
	}

	logger.WithFields(logrus.Fields{
		"host": cfg.GetMiddlewareConfig().GetHost(),
		"port": cfg.GetMiddlewareConfig().GetPort(),
		"user": cfg.GetMiddlewareConfig().GetUsername(),
	}).Info("Server initialized - ready to consume from data-dispatcher-connections queue")

	return server, nil
}

// Start starts consuming client notification messages from the existing queue
func (s *Server) Start() error {
	// Start the listener to consume messages and spawn goroutines
	// for each connection packet received
	err := s.listener.Start(s.shutdown, &s.clientWg)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	return nil
}

// Stop gracefully stops the server and waits for all client goroutines to finish
func (s *Server) Stop() {
	s.logger.Info("Initiating graceful server shutdown")
	
	// Close middleware (RabbitMQ connection and channel)
	s.middleware.Close()
	
	// Signal all client goroutines to stop
	close(s.shutdown)

	// Wait for all client goroutines to finish
	s.clientWg.Wait()


	s.logger.Info("Server shutdown completed")
}
