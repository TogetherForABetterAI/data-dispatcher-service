package server

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/sirupsen/logrus"
)

// Server handles RabbitMQ server operations
type Server struct {
	middleware      *middleware.Middleware
	logger          *logrus.Logger
	listener        *Listener
	config          config.Interface
	shutdownOnce    sync.Once                // Ensures Stop() is called only once
	shutdownHandler ShutdownHandlerInterface // Handles graceful shutdown logic
}

// NewServer creates a new RabbitMQ server
func NewServer(cfg config.Interface) (*Server, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	mw, err := middleware.NewMiddleware(cfg.GetMiddlewareConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware: %w", err)
	}

	server := &Server{
		middleware: mw,
		logger:     logger,
		config:     cfg,
	}

	realFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
		return NewClientManager(cfg, mw, clientID)
	}

	listener := NewListener(mw, cfg, realFactory)

	server.listener = listener

	// Initialize shutdown handler
	server.shutdownHandler = NewShutdownHandler(
		logger,
		listener,
		mw,
	)

	logger.WithFields(logrus.Fields{
		"host":        cfg.GetMiddlewareConfig().GetHost(),
		"port":        cfg.GetMiddlewareConfig().GetPort(),
		"user":        cfg.GetMiddlewareConfig().GetUsername(),
		"pod_name":    cfg.GetPodName(),
		"worker_pool": cfg.GetWorkerPoolSize(),
	}).Info("Server initialized successfully")

	return server, nil
}

func (s *Server) Run() error {
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)

	serverDone := s.startServerGoroutine()

	return s.shutdownHandler.HandleShutdown(serverDone, osSignals)
}

func (s *Server) startServerGoroutine() chan error {
	serverDone := make(chan error, 1)
	go func() {
		s.logger.WithFields(logrus.Fields{
			"pod_name":    s.config.GetPodName(),
			"worker_pool": s.config.GetWorkerPoolSize(),
		}).Info("Starting data dispatcher service")

		err := s.startComponents()
		serverDone <- err
	}()
	return serverDone
}

// main function
func (s *Server) startComponents() error {
	err := s.listener.Start() // this is the main blocking call
	if err != nil {
		if err.Error() == "context canceled" {
			s.logger.Info("Listener stopped consuming gracefully.")
			return nil
		}
		return fmt.Errorf("failed to start consuming: %w", err)
	}
	return nil
}
