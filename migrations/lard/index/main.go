package index

import (
	"fmt"
	"migrate/utils"

	"github.com/joho/godotenv"
)

type Config struct {
	Action   string `arg:"positional" help:"Valid choices: [\"drop\", \"create\"]"`
	Database string `arg:"-d" help:"Which database to operate on (all by default). Choice: [lard, lard_restricted]"`
	Table    string `arg:"-t" help:"Which table to operate on (all by default). Choice: [data, nonscalar_data]"`
}

type SelectOptions struct {
	data bool
	text bool
}

func newOptions(config *Config) (opts SelectOptions) {
	if utils.StringIsEmptyOrEqual(config.Table, "data") {
		opts.data = true
	}
	if utils.StringIsEmptyOrEqual(config.Table, "nonscalar_data") {
		opts.text = true
	}
	return
}

func (config *Config) Execute() error {
	err := godotenv.Load()
	if err != nil {
		return err
	}

	opts := newOptions(config)

	switch config.Action {
	case "drop":
		DropIndices(config.Database, opts)
	case "create":
		CreateIndices(config.Database, opts)
	default:
		return fmt.Errorf("Invalid argumnent '%s'", config.Action)
	}
	return nil
}
