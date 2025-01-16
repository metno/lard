package port

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	kvalobs "migrate/kvalobs/db"
	"migrate/lard"
	"migrate/utils"
)

type Config struct {
	kvalobs.BaseConfig
	SpanDir    string `arg:"--span" help:"Specific timespan directory to import. If empty all timespan directories will be processed"`
	MaxWorkers int    `arg:"-n" default:"10" help:"Max number of workers"`
}

func (Config) Description() string {
	return `Import Kvalobs tables into LARD.
The following environement variables need to set:
	- "LARD_CONN_STRING"
    - "STINFO_CONN_STRING"
    - "HISTKVALOBS_CONN_STRING"`
}

func (config *Config) Execute() {
	if err := config.CheckSpelling(); err != nil {
		fmt.Println(err)
		return
	}

	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	pool, err := pgxpool.New(context.Background(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
	}
	defer pool.Close()

	dbs := InitImportDBs()
	for name, db := range dbs {
		if !utils.StringIsEmptyOrEqual(config.Database, name) {
			continue
		}

		// Do this outside the loop and only cache from histkvalobs?
		cache := NewCache(db)
		db.Import(cache, pool, config)
	}
}
