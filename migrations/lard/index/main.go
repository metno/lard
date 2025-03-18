package index

import (
	"context"
	"fmt"

	"github.com/joho/godotenv"

	"migrate/lard"
)

type Config struct {
	Action   string `arg:"positional" help:"Valid choices: [\"drop\", \"create\"]"`
	Database string `arg:"-d" help:"Which database to operate on (all by default). Choice: [open, restricted]"`
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
		DropIndices(pools, config.Database)
	case "create":
		CreateIndices(pools, config.Database)
	default:
		return fmt.Errorf("Invalid argumnent '%s'", config.Action)
	}
	return nil
}
