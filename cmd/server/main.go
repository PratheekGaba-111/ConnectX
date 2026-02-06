package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"connectx/internal/server"
)

func main() {
	srv, cleanup, err := server.New()
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: srv.Routes(),
	}

	go func() {
		log.Printf("server listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
