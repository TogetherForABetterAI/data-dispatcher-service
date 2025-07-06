# Data Dispatcher Service

A gRPC microservice that handles authenticated client notifications and dispatches dataset batches to RabbitMQ queues.

## Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- **The dataset-generator-service (gRPC server) must be running before starting this microservice.**
- **RabbitMQ server must be accessible for publishing data batches.**

## About This Service

**Data Dispatcher Service** is a gRPC server written in Go that acts as a data distribution hub. Its main responsibilities are:

- **gRPC API:**
  - Exposes a `NotifyNewClient` RPC method that authentication backends can call when a new client is successfully authenticated.
  - Provides health check endpoints for monitoring.

- **Asynchronous Processing:**
  - Upon receiving a new client notification, immediately acknowledges the request and spawns a goroutine to handle data processing.
  - Each client is processed independently with proper context management and graceful shutdown support.

- **Dataset Integration:**
  - Communicates with the `dataset-generator-service` via gRPC to fetch data batches.
  - Supports configurable datasets (MNIST, CIFAR-10, etc.) and batch sizes.

- **RabbitMQ Publishing:**
  - Publishes fetched data batches to client-specific RabbitMQ queues.
  - Includes retry logic with exponential backoff for reliable message delivery.
  - Each message contains batch metadata and is marked as persistent.

- **Observability:**
  - Structured JSON logging with configurable log levels.
  - Comprehensive error handling and monitoring capabilities.

**New Architecture Overview:**
```
Authentication Backend -> gRPC NotifyNewClient -> [Data Dispatcher Service] -> dataset-generator-service
                                                           |
                                                           v
                                                   RabbitMQ Queue (per client)
```

## gRPC Interface

### NotifyNewClient

Called by authentication backends when a new client is authenticated:

```protobuf
rpc NotifyNewClient(NewClientRequest) returns (NewClientResponse);

message NewClientRequest {
  string client_id = 1;    // Unique identifier of the client
  string queue_name = 2;   // Name of the RabbitMQ queue where data should be published
  string amqp_host = 3;    // RabbitMQ hostname or IP
  int32 amqp_port = 4;     // RabbitMQ port (default: 5672)
  string amqp_user = 5;    // RabbitMQ username
  string amqp_pass = 6;    // RabbitMQ password
}

message NewClientResponse {
  string status = 1;       // Status of the operation (e.g., "OK", "ERROR")
  string message = 2;      // Optional message or error description
}
```

### Example Usage

```go
// Connect to the service
conn, err := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := pb.NewDataDispatcherServiceClient(conn)

// Notify about a new client
response, err := client.NotifyNewClient(ctx, &pb.NewClientRequest{
    ClientId:  "user123",
    QueueName: "user123-data-queue",
    AmqpHost:  "rabbitmq.example.com",
    AmqpPort:  5672,
    AmqpUser:  "datauser",
    AmqpPass:  "password",
})
```

## Configuration

See [CONFIG.md](CONFIG.md) for detailed configuration options.

Key environment variables:
- `GRPC_PORT`: Server port (default: 8080)
- `DATASET_SERVICE_ADDR`: Dataset service address (default: localhost:50051)
- `LOG_LEVEL`: Logging level (default: info)

## Running the Server

### Local Development

```bash
# Install dependencies
go mod tidy

# Run the server
go run src/main.go
```

### Docker Compose


1. **Build and start the server:**
   ```bash
   sudo docker compose up --build data-dispatcher-service
   ```
   This will build the Docker image and start the server on port 8080.

2. **Stop the server:**
   ```bash
   sudo docker compose down
   ```

## Data Flow

1. **Authentication Backend** calls `NotifyNewClient` with client details and RabbitMQ credentials
2. **Data Dispatcher Service** immediately responds with "OK" and spawns a goroutine
3. **Goroutine** connects to the dataset-generator-service and fetches data batches
4. **Each batch** is published to the client's dedicated RabbitMQ queue
5. **Process continues** until all dataset batches are published or context is cancelled

## RabbitMQ Message Format

Published messages contain:

```json
{
  "client_id": "user123",
  "batch_index": 42,
  "data": "<base64-encoded-batch-data>",
  "is_last_batch": false,
  "timestamp": "2024-01-15T10:30:00Z"
}
```

## Health Monitoring

The service provides health check endpoints:

```bash
# Using grpcurl
grpcurl -plaintext localhost:8080 data_dispatcher_service.DataDispatcherService/HealthCheck
```


