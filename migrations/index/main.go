package index

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5"

	"migrate/lard"
)

type Config struct {
	Action string `arg:"positional" help:"Valid choices: [\"drop\", \"create\"]"`
}

func (config *Config) Execute() error {
	conn, err := pgx.Connect(context.Background(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Lard:", err))
		return nil
	}
	defer conn.Close(context.Background())

	switch config.Action {
	case "drop":
		lard.DropIndices(conn)
	case "create":
		lard.CreateIndices(conn)
	default:
		return fmt.Errorf("Invalid argumnent '%s'", config.Action)
	}
	return nil
}
