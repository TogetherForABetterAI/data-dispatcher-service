package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/middleware"
	"github.com/data-dispatcher-service/src/models"
	"github.com/sirupsen/logrus"
)

type Orchestrator interface {
	RequestShutdown()
	RequestScaleUp()
}

// Server handles RabbitMQ server operations
type Server struct {
	middleware      *middleware.Middleware
	logger          *logrus.Logger
	listener        *Listener
	monitor         *ReplicaMonitor
	config          config.Interface
	shutdownRequest chan struct{}         // Channel to receive the shutdown request
	shutdownOnce    sync.Once             // Ensures Stop() is called only once
	scalePublisher  *middleware.Publisher // Publisher for scaling requests
	shutdownHandler ShutdownHandlerInterface       // Handles graceful shutdown logic
}

// NewServer creates a new RabbitMQ server
func NewServer(cfg config.Interface) (*Server, error) {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	mw, err := middleware.NewMiddleware(cfg.GetMiddlewareConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create middleware: %w", err)
	}

	shutdownReqChan := make(chan struct{}, 1)

	publisher, err := middleware.NewPublisher(mw.Conn())

	if err != nil {
		mw.Close()
		return nil, fmt.Errorf("failed to create scale publisher: %w", err)
	}

	server := &Server{
		middleware:      mw,
		logger:          logger,
		config:          cfg,
		shutdownRequest: shutdownReqChan,
		scalePublisher:  publisher,
	}

	monitor := NewReplicaMonitor(cfg, logger, server)

	realFactory := func(cfg config.Interface, mw middleware.MiddlewareInterface, clientID string) ClientManagerInterface {
		return NewClientManager(cfg, mw, clientID)
	}

	listener := NewListener(mw, cfg, monitor, realFactory)

	server.monitor = monitor
	server.listener = listener

	// Initialize shutdown handler
	server.shutdownHandler = NewShutdownHandler(
		logger,
		listener,
		monitor,
		mw,
		shutdownReqChan,
	)

	logger.WithFields(logrus.Fields{
		"host": cfg.GetMiddlewareConfig().GetHost(),
		"port": cfg.GetMiddlewareConfig().GetPort(),
		"user": cfg.GetMiddlewareConfig().GetUsername(),
	}).Info("Server initialized")

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
		slog.Info("Starting service",
			"service", s.config.GetConsumerTag(),
			"is_leader", s.config.IsLeader(),
			"min_threshold", s.config.GetMinThreshold())

		err := s.startComponents()
		serverDone <- err
	}()
	return serverDone
}

// main function
func (s *Server) startComponents() error {
	s.monitor.Start()
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

// RequestShutdown allows the ReplicaMonitor to request a server shutdown
func (s *Server) RequestShutdown() {
	s.shutdownOnce.Do(func() {
		close(s.shutdownRequest)
	})
}

// RequestScaleUp allows the ReplicaMonitor to request scaling up a service type
func (s *Server) RequestScaleUp() {

	msg := models.ScaleMessage{ReplicaType: s.config.GetReplicaName()}
	body, err := json.Marshal(msg)
	if err != nil {
		s.logger.WithField("error", err).Error("Failed to marshal scale-up message")
		return
	}
	err = s.scalePublisher.Publish(
		"",
		body,
		config.SCALABILITY_EXCHANGE,
	)
	if err != nil {
		s.logger.WithField("error", err).Error("Failed to publish scale-up message")
	}
}


