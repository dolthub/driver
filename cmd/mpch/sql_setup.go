package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cenkalti/backoff/v4"
	embedded "github.com/dolthub/driver"
)

func ensureSQLSchema(ctx context.Context, cfg harnessConfig) error {
	if cfg.DSN == "" {
		return fmt.Errorf("--dsn is required for --worker-mode=sql")
	}
	if cfg.WorkerDB == "" || cfg.WorkerTable == "" {
		return fmt.Errorf("worker db/table cannot be empty")
	}

	ecfg, err := embedded.ParseDSN(cfg.DSN)
	if err != nil {
		return err
	}
	// Avoid selecting DB before it exists.
	ecfg.Database = ""
	// Ensure schema setup doesn't hold onto singleton-cached engine instances.
	// (A non-nil BackOff triggers DisableSingletonCache in the connector open path.)
	ecfg.BackOff = backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 0)

	connector, err := embedded.NewConnector(ecfg)
	if err != nil {
		return err
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", cfg.WorkerDB)); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("USE %s", cfg.WorkerDB)); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id BIGINT PRIMARY KEY AUTO_INCREMENT, worker_id VARCHAR(64), ts BIGINT)",
		cfg.WorkerTable,
	)); err != nil {
		return err
	}

	return nil
}
