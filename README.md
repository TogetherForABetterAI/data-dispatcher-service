# Data Dispatcher Service

This service provides gRPC endpoints for streaming and retrieving dataset batches, as well as dataset metadata and health checks.

## Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Generating Go code from Protobuf 
For easier proto regeneration, you can use the provided script:

```bash
# Make the script executable (first time only)
chmod +x src/scripts/regenerate_proto.sh

# Run the script to regenerate proto files
sudo src/scripts/generate_proto.sh
```

This script will:
- Delete the old generated files in `src/pb/`
- Create a fresh `src/pb/` directory
- Run the proto-gen container to regenerate the code

## Running the Server with Docker Compose



1. **Build and start the server:**
   ```bash
   sudo docker compose up --build data-dispatcher-service
   ```
   This will build the Docker image (if needed) and start the server, exposing it on port 8080.

2. **Stop the server:**
   ```bash
   sudo docker compose down
   ```

## Notes
- If you add or change dependencies, update your `go.mod` and `go.sum` locally, then rebuild the Docker images.
- Generated Go code is placed in `src/pb/` by default.
