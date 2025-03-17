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
	SpanDir        string `arg:"--span" help:"Specific timespan directory to import. If empty all timespan directories will be processed"`
	MaxWorkers     int    `arg:"-n" default:"10" help:"Max number of workers"`
	SkipRestricted bool   `help:"Skip import of restricted data"`
	SkipOpen       bool   `help:"Skip import of open data"`
}

func (Config) Description() string {
	return `Import Kvalobs tables into LARD.
The following environement variables need to set:
    - "LARD_OPEN_CONN_STRING"
    - "LARD_RESTRICTED_CONN_STRING"
    - "STINFO_CONN_STRING"
    - "HISTKVALOBS_CONN_STRING"`
}

func (config *Config) Execute() {
	utils.GoMemLimitMessage("kvalobs")

	if err := config.CheckSpelling(); err != nil {
		fmt.Println(err)
		return
	}

	err := godotenv.Load()
	if err != nil {
		fmt.Println(config.Description())
		return
	}

	// Create lard connection pools
	pools := lard.NewLardPool(context.Background())
	defer pools.Close()

	cache := NewCache()
	tables := InitImportTables()

	for _, table := range tables {
		if !utils.StringIsEmptyOrEqual(config.Database, table.DbName) ||
			!utils.StringIsEmptyOrEqual(config.Table, table.Name) {
			continue
		}

		cache.CacheMetadata(table)

		if config.SpanDir == "" {
			table.ImportAllTimespans(cache, pools, config)
		} else {
			table.Import(cache, pools, config)
		}
	}
}
