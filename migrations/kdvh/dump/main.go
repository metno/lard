package dump

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"

	kdvh "migrate/kdvh/db"
	"migrate/utils"
)

type Config struct {
	kdvh.BaseConfig
	Overwrite bool `help:"Overwrite any existing dumped files"`
	MaxConn   int  `arg:"-n" default:"4" help:"Max number of allowed concurrent connections to KDVH"`
}

func (Config) Description() string {
	return `Dump tables from KDVH.
The \"KDVH_PROXY_CONN_STRING\" environement variable is required for this command`
}

func (config *Config) Execute() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	pool, err := pgxpool.New(context.Background(), os.Getenv(kdvh.KDVH_ENV_VAR))
	if err != nil {
		log.Error().Err(err).Msg("")
		return
	}

	tables := InitDump()
	for _, table := range tables {
		if !config.ShouldProcessTable(table.TableName) {
			continue
		}

		// TODO: need to mkdir if we want to pass config.Path here
		handle := utils.SetLoggerOutput(table.TableName, "dump")
		defer handle.Close()

		table.Dump(pool, config)
	}
}
