package port

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	kdvh "migrate/kdvh/db"
	"migrate/lard"
)

type Config struct {
	Verbose bool `arg:"-v" help:"Increase verbosity level"`
	kdvh.BaseConfig
	Sep        string `default:"," help:"Separator character in the dumped files. Needs to be quoted"`
	NoHeader   bool   `help:"Add this flag if the dumped CSV files do not have a header row"`
	MaxWorkers int    `arg:"-n" default:"10" help:"Max number of workers"`
	// TODO: this isn't implemented in go-arg
	// Skip      string   `choice:"data" choice:"flags" help:"Skip import of data or flags"`
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

	fmt.Println("Import started!")
	database := InitImportTables()

	// Cache metadata from Stinfosys, KDVH, and local `product_offsets.csv`
	cache := CacheMetadata(config.Tables, config.Stations, config.Elements, database)

	// Create connection pool for LARD
	pool, err := pgxpool.New(context.TODO(), os.Getenv(lard.LARD_ENV_VAR))
	if err != nil {
		fmt.Println("Could not connect to Lard")
		return
	}
	defer pool.Close()

	for _, table := range database {
		if !config.ShouldProcessTable(table.TableName) {
			continue
		}

		table.Import(cache, pool, config)
	}

	fmt.Println("Import complete!")
}
