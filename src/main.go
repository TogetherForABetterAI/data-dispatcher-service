package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/data-dispatcher-service/src/config"
	"github.com/data-dispatcher-service/src/server"
)

func loadConfig() config.GlobalConfig {
	config, err := config.NewConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	return config
}

func setupLogging(config config.GlobalConfig) {
	logLevel := slog.LevelInfo
	switch config.GetLogLevel() {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)
}

func main() {
	config := loadConfig()
	setupLogging(config)

	srv, err := server.NewServer(config)
	if err != nil {
		slog.Error("Failed to initialize server", "error", err)
		os.Exit(1)
	}
	startServiceWithGracefulShutdown(srv, config)

	slog.Info("Service shutdown complete. Exiting.")
}

// startServiceWithGracefulShutdown orchestrates the service lifecycle and handles graceful shutdown.
func startServiceWithGracefulShutdown(srv *server.Server, config config.GlobalConfig) {
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, syscall.SIGINT, syscall.SIGTERM)
	serverDone := startServer(srv, config)
	handleShutdown(srv, serverDone, osSignals)
}

// startServerAsync starts the server in a goroutine and returns a channel for server errors.
func startServer(srv *server.Server, config config.GlobalConfig) chan error {
	serverDone := make(chan error, 1)
	go func() {
		slog.Info("Starting service",
			"service", config.GetConsumerTag(),
			"is_leader", config.IsLeader(),
			"min_threshold", config.GetMinThreshold())
		err := srv.Start()
		serverDone <- err // Notify main routine when finished
	}()
	return serverDone
}

// handleShutdown waits for shutdown signals and orchestrates graceful shutdown.
func handleShutdown(srv *server.Server, serverDone chan error, osSignals chan os.Signal) {
	select {
	case err := <-serverDone:
		if err != nil {
			slog.Error("Service stopped unexpectedly due to an error", "error", err)
			srv.ShutdownClients(true)
			os.Exit(1)
		}
		slog.Warn("Service stopped unexpectedly without an error.")
	case <-osSignals:
		slog.Info("Received OS signal. Initiating graceful shutdown...")
		srv.ShutdownClients(true)
		<-serverDone
		slog.Info("Service exited gracefully after receiving OS signal.")
	case <-srv.GetShutdownChan():
		slog.Info("Received internal scale-in request. Initiating graceful shutdown...")
		srv.ShutdownClients(false) // wait for clients to finish
		<-serverDone
		slog.Info("Service exited gracefully after internal scale-in request.")
	}
}
