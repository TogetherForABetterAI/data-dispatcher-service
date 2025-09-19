package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type GlobalConfig struct {
	LogLevel         string
	ServiceName      string
	MiddlewareConfig *MiddlewareConfig
	GrpcConfig       *GrpcConfig
}

type GrpcConfig struct {
	DatasetServiceAddr string
	DatasetName        string
	BatchSize          int32
}

// MiddlewareConfig holds RabbitMQ connection configuration
type MiddlewareConfig struct {
	Host       string
	Port       int32
	Username   string
	Password   string
	MaxRetries int
}

func NewConfig() (GlobalConfig, error) {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		return GlobalConfig{}, fmt.Errorf("failed to load .env file: %w", err)
	}

	// Get RabbitMQ connection details from environment
	rabbitHost := os.Getenv("RABBITMQ_HOST")
	if rabbitHost == "" {
		return GlobalConfig{}, fmt.Errorf("RABBITMQ_HOST environment variable is required")
	}

	rabbitPortStr := os.Getenv("RABBITMQ_PORT")
	if rabbitPortStr == "" {
		return GlobalConfig{}, fmt.Errorf("RABBITMQ_PORT environment variable is required")
	}
	rabbitPort, err := strconv.ParseInt(rabbitPortStr, 10, 32)
	if err != nil {
		return GlobalConfig{}, fmt.Errorf("RABBITMQ_PORT must be a valid integer: %w", err)
	}

	rabbitUser := os.Getenv("RABBITMQ_USER")
	if rabbitUser == "" {
		return GlobalConfig{}, fmt.Errorf("RABBITMQ_USER environment variable is required")
	}

	rabbitPass := os.Getenv("RABBITMQ_PASS")
	if rabbitPass == "" {
		return GlobalConfig{}, fmt.Errorf("RABBITMQ_PASS environment variable is required")
	}

	// Set log level from environment
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		return GlobalConfig{}, fmt.Errorf("LOG_LEVEL environment variable is required")
	}

	// Get dataset service address from environment
	datasetAddr := os.Getenv("DATASET_SERVICE_ADDR")
	if datasetAddr == "" {
		return GlobalConfig{}, fmt.Errorf("DATASET_SERVICE_ADDR environment variable is required")
	}

	// Get dataset name from environment
	datasetName := os.Getenv("DATASET_NAME")
	if datasetName == "" {
		return GlobalConfig{}, fmt.Errorf("DATASET_NAME environment variable is required")
	}

	// Get batch size from environment
	batchSizeStr := os.Getenv("BATCH_SIZE")
	if batchSizeStr == "" {
		return GlobalConfig{}, fmt.Errorf("BATCH_SIZE environment variable is required")
	}
	batchSize, err := strconv.ParseInt(batchSizeStr, 10, 32)
	if err != nil {
		return GlobalConfig{}, fmt.Errorf("BATCH_SIZE must be a valid integer: %w", err)
	}

	// Get max retries from environment (optional with default)
	maxRetries := 3
	if retriesStr := os.Getenv("MAX_RETRIES"); retriesStr != "" {
		parsed, err := strconv.Atoi(retriesStr)
		if err != nil {
			return GlobalConfig{}, fmt.Errorf("MAX_RETRIES must be a valid integer: %w", err)
		}
		maxRetries = parsed
	}

	return GlobalConfig{
		LogLevel:    logLevel,
		ServiceName: "data-dispatcher-service",
		MiddlewareConfig: &MiddlewareConfig{
			Host:       rabbitHost,
			Port:       int32(rabbitPort),
			Username:   rabbitUser,
			Password:   rabbitPass,
			MaxRetries: maxRetries,
		},
		GrpcConfig: &GrpcConfig{
			DatasetServiceAddr: datasetAddr,
			DatasetName:        datasetName,
			BatchSize:          int32(batchSize),
		},
	}, nil
}

var Config GlobalConfig

func init() {
	config, err := NewConfig()
	if err != nil {
		panic(fmt.Sprintf("Failed to load configuration: %v", err))
	}
	Config = config
}

const (
	DATASET_EXCHANGE = "dataset-exchange"
)
