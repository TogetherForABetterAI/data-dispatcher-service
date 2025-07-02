# Data Dispatcher Service

This service provides gRPC endpoints for streaming and retrieving dataset batches, as well as dataset metadata and health checks.

## Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Generating Go code from Protobuf (using Docker)
If you modify the proto files, you can regenerate the Go code using Docker Compose:

```bash
sudo docker compose up --build proto-gen
```

This will generate the Go protobuf and gRPC files into `src/pb/`.

## Running the Server with Docker Compose

1. **Generate the protobuf files (if not already done):**
   ```bash
   sudo docker compose up --build proto-gen
   ```
   (You can stop the proto-gen container after generation with `sudo docker compose down`)

2. **Build and start the server:**
   ```bash
   sudo docker compose up --build go-server
   ```
   This will build the Docker image (if needed) and start the server, exposing it on port 8080.

3. **Stop the server:**
   ```bash
   sudo docker compose down
   ```

## Notes
- If you add or change dependencies, update your `go.mod` and `go.sum` locally, then rebuild the Docker images.
- Generated Go code is placed in `src/pb/` by default.
