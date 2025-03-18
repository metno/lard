package index

import (
	"fmt"

	"github.com/joho/godotenv"
)

type Config struct {
	Action   string `arg:"positional" help:"Valid choices: [\"drop\", \"create\"]"`
	Database string `arg:"-d" help:"Which database to operate on (all by default). Choice: [lard, lard_restricted]"`
}

func (config *Config) Execute() error {
	err := godotenv.Load()
	if err != nil {
		return err
	}

	switch config.Action {
	case "drop":
		DropIndices(config.Database)
	case "create":
		CreateIndices(config.Database)
	default:
		return fmt.Errorf("Invalid argumnent '%s'", config.Action)
	}
	return nil
}
