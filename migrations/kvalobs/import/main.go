package port

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	kvalobs "migrate/kvalobs/db"
	"migrate/kvalobs/import/cache"
	"migrate/lard"
	"migrate/utils"
)

type Config struct {
	kvalobs.BaseConfig
}

func (Config) Description() string {
	return `Import Kvalobs tables into LARD.
The following environement variables need to set:
	- "LARD_CONN_STRING"
    - "STINFO_CONN_STRING"
    - "HISTKVALOBS_CONN_STRING"`
}

func (config *Config) Execute() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	dbs := kvalobs.InitDBs()
	// Only cache from histkvalobs?
	cache := cache.New(dbs["histkvalobs"])

	pool, err := pgxpool.New(context.Background(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
	}
	defer pool.Close()

	for name, db := range dbs {
		if !utils.StringIsEmptyOrEqual(config.Database, name) {
			continue
		}
		ImportDB(db, cache, pool, config)

	}
}
