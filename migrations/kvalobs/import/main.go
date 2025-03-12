package port

import (
	"context"
	"fmt"

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

	// Create lard connection pools
	pool := lard.NewLardPool(context.Background())
	defer pool.Close()

	cache := NewCache()
	tables := InitImportTables()

	for _, table := range tables {
		if !utils.StringIsEmptyOrEqual(config.Database, table.DbName) ||
			!utils.StringIsEmptyOrEqual(config.Table, table.Name) {
			continue
		}

		cache.CacheMetadata(table)

		if config.SpanDir == "" {
			table.ImportAllTimespans(cache, pool, config)
		} else {
			table.Import(cache, pool, config)
		}
	}
}
