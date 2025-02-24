package index

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"

	"migrate/lard"
)

type Config struct {
	Action string `arg:"positional" help:"Valid choices: [\"drop\", \"create\"]"`
}

func (config *Config) Execute() error {
	err := godotenv.Load()
	if err != nil {
		return err
	}

	conn, err := pgx.Connect(context.Background(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		log.Error().Err(err).Msg("Could not connect to Lard")
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
