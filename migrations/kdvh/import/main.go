package port

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"

	kdvh "migrate/kdvh/db"
	"migrate/lard"
	"migrate/utils"
)

const KDVH_TABLE_DIR_PREFIX string = "T_"

type Config struct {
	kdvh.BaseConfig
	Sep            string `default:"," help:"Separator character in the dumped files. Needs to be quoted"`
	NoHeader       bool   `help:"Add this flag if the dumped CSV files do not have a header row"`
	MaxWorkers     int    `arg:"-n" default:"10" help:"Max number of workers"`
	SkipRestricted bool   `help:"Skip import of restricted data"`
	SkipOpen       bool   `help:"Skip import of open data"`
	SkipScalar     bool   `help:"Skip import of scalar data"`
	SkipText       bool   `help:"Skip import of text data"`
	Verbose        bool   `arg:"-v" help:"Increase verbosity level"`
}

func (Config) Description() string {
	return `Import KDVH tables into LARD.
The following environement variables need to set:
    - "LARD_CONN_STRING"
    - "LARD_RESTRICTED_CONN_STRING"
    - "STINFO_CONN_STRING"`
}

func (config *Config) Execute() {
	if os.Getenv("GOMEMLIMIT") == "" {
		utils.PrintGoMemLimitMessage("kdvh")
		os.Exit(1)
	}

	err := godotenv.Load()
	if err != nil {
		fmt.Println(config.Description())
		os.Exit(1)
	}

	if len([]rune(config.Sep)) > 1 {
		fmt.Printf("Error: '--sep' only accepts single-byte characters. Got %s", config.Sep)
		os.Exit(1)
	}

	fmt.Println("Import started!")
	// Cache metadata from Stinfosys and local `product_offsets.csv`
	cache := CacheMetadata()

	// Create connection pools for LARD
	pools := lard.NewLardPool(context.Background())
	defer pools.Close()

	// Recurse from config.Path to directories below
	recursePath(config.Path, cache, pools, config)
	fmt.Println("Import complete!")
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

		table, ok := IMPORT_TABLES[dirname]
		if !ok {
			if strings.HasPrefix(entry.Name(), KDVH_TABLE_DIR_PREFIX) {
				continue
			}
			recursePath(dirpath, cache, pools, config)
			continue
		}

		if !config.ShouldProcessTable(table.Name) {
			continue
		}

		fmt.Println("importing from", dirpath)
		table.Import(dirpath, cache, pools, config)

		fmt.Println(strings.Repeat("- ", 40))
	}
}
