FROM golang:1.23.10

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/src

EXPOSE 8080

CMD ["go", "run", "main.go"]