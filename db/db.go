package db

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

var DB *pgxpool.Pool

func Init() error {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL is not set")
	}

	var err error
	DB, err = pgxpool.New(context.Background(), dbURL)
	if err != nil {
		return fmt.Errorf("failed to create db pool: %w", err)
	}

	return nil
}
