package migrate

import (
	"context"
	"fmt"
	"kdvh_importer/lard"
	"log/slog"
	"os"
	"strings"
	// "time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	System      string
	Verbose     bool     `short:"v" description:"Increase verbosity level"`
	BaseDir     string   `long:"dir" default:"./" description:"Base directory where the dumped data is stored"`
	Sep         string   `long:"sep" default:","  description:"Separator character in the dumped files. Needs to be quoted"`
	TablesCmd   string   `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationsCmd string   `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElementsCmd string   `long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	HasHeader   bool     `long:"has-header" description:"Add this flag if the dumped files have a header row"`
	SkipData    bool     `long:"skip-data" description:"Skip import of data"`
	SkipFlags   bool     `long:"skip-flags" description:"Skiph import of flags"`
	Email       []string `long:"email" description:"Optional email address used to notify if the program crashed"`
	Tables      []string
	Stations    []string
	Elements    []string
	// TODO: move these to KDVH specific config
	// OffsetMap map[ParamKey]period.Period // Map of offsets used to correct (?) KDVH times for specific parameters
	// StinfoMap map[ParamKey]Metadata      // Map of metadata used to query timeseries ID in LARD
	// KDVHMap   map[KDVHKey]*MetaKDVH      // Map of from_time and to_time for each (table, station, element) triplet
}

// Sets up config:
// - Checks validity of cmd args
// - Populates slices by parsing the strings provided via cmd
// - Caches time offsets by reading 'param_offset.csv'
// - Caches metadata from Stinfosys
// - Caches metadata from KDVH proxy
func (config *Config) setup() {
	if config.SkipData && config.SkipFlags {
		slog.Error("Both '--skip-data' and '--skip-flags' are set, nothing to import")
		os.Exit(1)
	}

	if len(config.Sep) > 1 {
		slog.Warn("'--sep' only accepts single-byte characters. Defaulting to ','")
		config.Sep = ","
	}

	if config.TablesCmd != "" {
		config.Tables = strings.Split(config.TablesCmd, ",")
	}
	if config.StationsCmd != "" {
		config.Stations = strings.Split(config.StationsCmd, ",")
	}
	if config.ElementsCmd != "" {
		config.Elements = strings.Split(config.ElementsCmd, ",")
	}
}

// This method is automatically called by go-flags while parsing the cmd
func (config *Config) Execute(_ []string) error {
	config.setup()

	// Create connection pool for LARD
	pool, err := pgxpool.New(context.TODO(), os.Getenv("LARD_STRING"))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Lard:", err))
	}
	defer pool.Close()

	return nil
}

type Migrator interface {
	convert() (lard.Obs, error)
}
