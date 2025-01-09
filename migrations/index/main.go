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
	pool, err := pgxpool.New(context.Background(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Lard:", err))
		return nil
	}
	defer pool.Close()

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
