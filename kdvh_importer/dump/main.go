package dump

import (
	"database/sql"
	"log/slog"
	"os"
	"slices"
	"strings"
)

type Config struct {
	// TODO: make system positional?
	System      string   `default:"all" choice:"kdvh" choice:"kvlaobs" choice:"all" description:"Name of the database you want to dump data from"`
	BaseDir     string   `long:"dir" default:"./" description:"Location the dumped data will be stored in"`
	TablesCmd   string   `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationsCmd string   `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElementsCmd string   `long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	Overwrite   bool     `long:"overwrite" description:"Overwrite any existing dumped files"`
	Email       []string `long:"email" description:"Optional email address used to notify if the program crashed"`
	Tables      []string
	Stations    []string
	Elements    []string
}

// Populates config slices by splitting cmd strings
func (config *Config) setup() {
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

func (config *Config) Execute(_ []string) error {
	config.setup()

	dumpKDVH(config)
	dumpKvalobs(config)

	return nil
}

func dumpKDVH(config *Config) error {
	// TODO: make sure we don't need direct KDVH connection
	// dvhConn := getDB(os.Getenv("DVH_STRING"))
	// klima11Conn := getDB(os.Getenv("KLIMA11_STRING"))

	conn, err := sql.Open("pgx", os.Getenv("KDVH_PROXY_CONN"))
	if err != nil {
		slog.Error(err.Error())
		return nil
	}

	for _, table := range KDVH_TABLES {
		if config.Tables != nil && !slices.Contains(config.Tables, table.TableName) {
			continue
		}
		table.dump(conn, config)
	}

	return nil
}

func dumpKvalobs(config *Config) error {
	// TODO:
	return nil
}

// TODO: This is only useful if the different tables are defined as separate structs?
type Dumper interface {
	Dump(conn *sql.DB, config *Config)
}
