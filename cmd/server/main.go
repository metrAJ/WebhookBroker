package main

import (
	"context"
	"log"
	"webhookbroker/internal/db"
)

func main() {
	ctx := context.Background()
	dsn := "postgres://user:password@localhost:5433/broker?sslmode=disable"
	pool, err := db.InitDB(ctx, dsn)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer pool.Close()

}
