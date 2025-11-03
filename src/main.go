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
	osShutdown := make(chan struct{}, 1)
	go func() {
		sig, ok := <-osSignals // block until an OS signal is received
		if !ok {
			return
		}
		slog.Info("Received OS signal. Initiating shutdown...", "signal", sig)
		srv.ShutdownClients(true) // interrupt ongoing processing
		osShutdown <- struct{}{}
	}()

	select {
	case err := <-serverDone:
		if err != nil {
			slog.Error("Service stopped unexpectedly due to an error", "error", err)
			srv.ShutdownClients(true)
			os.Exit(1) // exit with error code
		}
		slog.Info("Service stopped without an error.")
	case <-osShutdown:
		err := <-serverDone // wait for server to finish
		if err != nil {
			slog.Error("Service encountered an error during OS signal shutdown", "error", err)
			os.Exit(1)
		}
		slog.Info("Service exited gracefully after receiving OS signal.")
	case <-srv.GetShutdownChan():
		slog.Info("Received internal scale-in request. Initiating graceful shutdown...")
		srv.ShutdownClients(false) // wait for clients to finish
		<-serverDone               // wait for server to finish
		slog.Info("Service exited gracefully after internal scale-in request.")
	}
}
