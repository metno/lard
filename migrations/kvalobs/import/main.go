package port

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/joho/godotenv"

	kvalobs "migrate/kvalobs/db"
	"migrate/lard"
	"migrate/utils"
)

type Config struct {
	kvalobs.BaseConfig
	MaxWorkers     int  `arg:"-n" default:"10" help:"Max number of workers"`
	SkipRestricted bool `help:"Skip import of restricted data"`
	SkipOpen       bool `help:"Skip import of open data"`
}

func (Config) Description() string {
	return `Import Kvalobs tables into LARD.
The following environement variables need to set:
    - "LARD_OPEN_CONN_STRING"
    - "LARD_RESTRICTED_CONN_STRING"
    - "STINFO_CONN_STRING"`
}

func (config *Config) Execute() {
	if os.Getenv("GOMEMLIMIT") == "" {
		utils.PrintGoMemLimitMessage("kvalobs")
		os.Exit(1)
	}

	if err := config.CheckTableSpelling(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err := godotenv.Load()
	if err != nil {
		fmt.Println(config.Description())
		os.Exit(1)
	}

	// Create lard connection pools
	pools := lard.NewLardPool(context.Background())
	defer pools.Close()

	cache := NewCache()
	recursePath(config.Path, cache, pools, config)
}

func recursePath(path string, cache *Cache, pools *lard.Pools, config *Config) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		dirname := entry.Name()
		dirpath := filepath.Join(path, dirname)

		if !slices.Contains(kvalobs.TABLES, dirname) {
			recursePath(dirpath, cache, pools, config)
			continue
		}

		table := Table{dirname}
		if !utils.StringIsEmptyOrEqual(config.Table, table.Name) {
			continue
		}

		fmt.Println("Importing from", dirpath)
		table.Import(dirpath, cache, pools, config)

		fmt.Println(strings.Repeat("- ", 40))
	}
}
