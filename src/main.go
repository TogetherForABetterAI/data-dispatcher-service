package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mlops-eval/data-dispatcher-service/src/transport"
)

func main() {
	addr := ":8080"
	server := transport.NewTCPServer(addr)

	go server.Start()

	// Esperar señal de parada
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("Iniciando shutdown...")
	server.Stop()
	log.Println("Servidor detenido")
}
