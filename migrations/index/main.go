package index

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/lard"
)

type Config struct {
	Action string `arg:"positional" help:"Valid choices: [\"drop\", \"create\"]"`
}

func (config *Config) Execute() error {
	// Create connection pool for LARD
	pool, err := pgxpool.New(context.TODO(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Lard:", err))
		return nil
	}

	switch config.Action {
	case "drop":
		lard.DropIndices(pool)
	case "create":
		lard.CreateIndices(pool)
	default:
		return fmt.Errorf("Invalid argumnent '%s'", config.Action)
	}
	return nil
}
