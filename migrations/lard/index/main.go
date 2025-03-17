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

	pools := lard.NewLardPool(context.Background())
	defer pools.Close()

	switch config.Action {
	case "drop":
		DropIndices(pools)
	case "create":
		CreateIndices(pools)
	default:
		return fmt.Errorf("Invalid argumnent '%s'", config.Action)
	}
	return nil
}
