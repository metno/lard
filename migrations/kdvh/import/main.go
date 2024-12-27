package port

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	kdvh "migrate/kdvh/db"
	"migrate/kdvh/import/cache"
	"migrate/lard"
	"migrate/utils"
)

type Config struct {
	Verbose   bool     `arg:"-v" help:"Increase verbosity level"`
	Path      string   `arg:"-p" default:"./dumps/kdvh" help:"Location the dumped data will be stored in"`
	BaseDir   string   `arg:"-p,--path" default:"./dumps/kdvh" help:"Location the dumped data will be stored in"`
	Tables    []string `arg:"-t" help:"Optional space separated list of table names"`
	Stations  []string `arg:"-s" help:"Optional space separated list of stations IDs"`
	Elements  []string `arg:"-e" help:"Optional space separated list of element codes"`
	Sep       string   `default:"," help:"Separator character in the dumped files. Needs to be quoted"`
	HasHeader bool     `help:"Add this flag if the dumped files have a header row"`
	// TODO: this isn't implemented in go-arg
	// Skip      string   `choice:"data" choice:"flags" help:"Skip import of data or flags"`
	Reindex bool `help:"Drop PG indices before insertion. Might improve performance"`
}

func (Config) Description() string {
	return `Import KDVH tables into LARD.
The following environement variables need to set:
    - "LARD_CONN_STRING"
    - "STINFO_CONN_STRING"
    - "KDVH_PROXY_CONN_STRING"`
}

func (config *Config) Execute() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	if len(config.Sep) > 1 {
		fmt.Printf("Error: '--sep' only accepts single-byte characters. Got %s", config.Sep)
		os.Exit(1)
	}

	slog.Info("Import started!")
	database := kdvh.Init()

	// Cache metadata from Stinfosys, KDVH, and local `product_offsets.csv`
	cache := cache.CacheMetadata(config.Tables, config.Stations, config.Elements, database)

	// Create connection pool for LARD
	pool, err := pgxpool.New(context.TODO(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Lard:", err))
		return
	}
	defer pool.Close()

	if config.Reindex {
		utils.DropIndices(pool)
	}

	// Recreate indices even in case the main function panics
	defer func() {
		r := recover()
		if config.Reindex {
			utils.CreateIndices(pool)
		}

		if r != nil {
			panic(r)
		}
	}()

	for _, table := range database.Tables {
		if len(config.Tables) > 0 && !slices.Contains(config.Tables, table.TableName) {
			continue
		}

		if !table.ShouldImport() {
			if config.Verbose {
				slog.Info("Skipping import of " + table.TableName + " because this table is not set for import")
			}
			continue
		}

		utils.SetLogFile(table.TableName, "import")
		ImportTable(table, cache, pool, config)
	}

	log.SetOutput(os.Stdout)
	slog.Info("Import complete!")
}
