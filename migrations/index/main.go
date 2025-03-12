package index

import (
	"context"
	"fmt"

	"github.com/joho/godotenv"

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

	pool := lard.NewLardPool(context.TODO())
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
