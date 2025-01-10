package dump

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"migrate/kdvh/db"
	"migrate/utils"
)

type Config struct {
	Path      string   `arg:"-p" default:"./dumps/kdvh" help:"Location the dumped data will be stored in"`
	Tables    []string `arg:"-t" help:"Optional space separated list of table names"`
	Stations  []string `arg:"-s" help:"Optional space separated list of stations IDs"`
	Elements  []string `arg:"-e" help:"Optional space separated list of element codes"`
	Overwrite bool     `help:"Overwrite any existing dumped files"`
	MaxConn   int      `arg:"-n" default:"4" help:"Max number of allowed concurrent connections to KDVH"`
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

	pool, err := pgxpool.New(context.Background(), os.Getenv(db.KDVH_ENV_VAR))
	if err != nil {
		slog.Error(err.Error())
		return
	}

	kdvh := db.Init()
	for _, table := range kdvh.Tables {
		if len(config.Tables) > 0 && !slices.Contains(config.Tables, table.TableName) {
			continue
		}

		// TODO: need to mkdir if we want to pass config.Path here
		utils.SetLogFile(".", table.TableName+"_dump")
		DumpTable(table, pool, config)
	}
}
