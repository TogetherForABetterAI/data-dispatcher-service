package server

import (
	"fmt"
	"sync" 
	"github.com/mlops-eval/data-dispatcher-service/src/config"
	"github.com/mlops-eval/data-dispatcher-service/src/middleware"
	"github.com/sirupsen/logrus"
)

// Server handles RabbitMQ server operations
type Server struct {
	middleware      *middleware.Middleware
	logger          *logrus.Logger
	listener        *Listener
	monitor         *ReplicaMonitor
	config          config.Interface
	shutdownRequest chan struct{} // Channel to receive the shutdown request
	shutdownOnce    sync.Once     // Ensures Stop() is called only once
}

// NewServer creates a new RabbitMQ server
func NewServer(cfg config.Interface) (*Server, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	middleware, err := middleware.NewMiddleware(cfg.GetMiddlewareConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware: %w", err)
	}

	clientManager := NewClientManager(cfg, middleware.Conn(), middleware)

	shutdownReqChan := make(chan struct{}, 1)

	server := &Server{
		middleware:      middleware,
		logger:          logger,
		config:          cfg,
		shutdownRequest: shutdownReqChan,
	}

	monitor := NewReplicaMonitor(cfg, logger, shutdownReqChan)

	listener := NewListener(clientManager, middleware, cfg, monitor)

	server.monitor = monitor
	server.listener = listener

	logger.WithFields(logrus.Fields{
		"host": cfg.GetMiddlewareConfig().GetHost(),
		"port": cfg.GetMiddlewareConfig().GetPort(),
		"user": cfg.GetMiddlewareConfig().GetUsername(),
	}).Info("Server initialized")

	return server, nil
}

func (s *Server) ShutdownRequestChannel() <-chan struct{} {
	return s.shutdownRequest
}

// main function
func (s *Server) Start() error {
	s.monitor.Start()
	err := s.listener.Start()
	if err != nil {
		if err.Error() == "context canceled" {
			s.logger.Info("Listener stopped consuming gracefully.")
			return nil
		}
		return fmt.Errorf("failed to start consuming: %w", err)
	}
	return nil
}

// Stop gracefully shuts down the server
func (s *Server) Stop() {
	s.shutdownOnce.Do(func() { // "Do" ensures this method is only executed once
		s.logger.Info("Initiating graceful server shutdown...")
		s.middleware.StopConsuming(s.listener.GetConsumerTag())
		s.monitor.Stop()
		s.listener.Stop()
		s.middleware.Close()
		s.logger.Info("Server shutdown completed")
	})
}
