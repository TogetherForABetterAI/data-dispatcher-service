FROM golang:1.23.10 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download
  
COPY . .

WORKDIR /app/src
RUN go build -o app-binary main.go

FROM gcr.io/distroless/base-debian12

WORKDIR /app
COPY --from=builder /app/src/app-binary .

EXPOSE 8080
CMD ["./app-binary"]