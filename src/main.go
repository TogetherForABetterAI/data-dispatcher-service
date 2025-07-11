package main

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/mlops-eval/data-dispatcher-service/src/processor"
	"github.com/mlops-eval/data-dispatcher-service/src/grpc"
	"github.com/sirupsen/logrus"
)

func main() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		// It's okay if .env file doesn't exist
	}

	// Setup logging
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	
	// Set log level from environment
	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		if level, err := logrus.ParseLevel(logLevel); err == nil {
			logger.SetLevel(level)
		}
	}

	// Get port from environment or use default
	port := 8080
	if portStr := os.Getenv("GRPC_PORT"); portStr != "" {
		if parsed, err := strconv.Atoi(portStr); err == nil {
			port = parsed
		}
	}

	logger.WithFields(logrus.Fields{
		"service": "data-dispatcher-service",
		"version": "1.0.0",
		"port":    port,
	}).Info("Starting data dispatcher service")

	// Create client processor
	clientProcessor := processor.NewClientDataProcessor()

	// Create gRPC server
	grpcServer := grpc.NewDataDispatcherServer(clientProcessor)

	// Start server in a goroutine
	go func() {
		logger.WithField("port", port).Info("Starting gRPC server")
		if err := grpcServer.Start(port); err != nil {
			logger.WithError(err).Fatal("Failed to start gRPC server")
		}
	}()

	// Wait for shutdown signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	logger.Info("Received shutdown signal, initiating graceful shutdown")
	grpcServer.Stop()
	logger.Info("Server stopped successfully")
}
