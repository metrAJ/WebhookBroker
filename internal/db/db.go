package db

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"webhookbroker/internal/config"

	"github.com/golang-migrate/migrate/v4"

	// Register the postgres database driver for golang-migrate
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations
var migrationsFS embed.FS

func InitDB(ctx context.Context, cf config.DBConfig) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(cf.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database DSN: %w", err)
	}

	config.MaxConns = int32(cf.MaxConns)
	config.MinConns = int32(cf.MinConns)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	if err := runMigrations(cf.DSN); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	slog.Info("Database connection pool initialized successfully")

	return pool, nil
}

func runMigrations(dsn string) error {
	d, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return err
	}

	m, err := migrate.NewWithSourceInstance("iofs", d, dsn)
	if err != nil {
		return err
	}

	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		return err
	}

	slog.Info("Database migrations applied successfully")

	return nil
}
