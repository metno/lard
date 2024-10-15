package dump

import (
	"kdvh_importer/kdvh"
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
	config.System = strings.ToLower(config.System)

	if config.TablesCmd != "" {
		config.Tables = toLower(strings.Split(config.TablesCmd, ","))
	}
	if config.StationsCmd != "" {
		config.Stations = strings.Split(config.StationsCmd, ",")
	}
	if config.ElementsCmd != "" {
		// TODO: maybe avoid toLower here? At least for insertion?
		config.Elements = toLower(strings.Split(config.ElementsCmd, ","))
	}
}

// NOTE: needed for postgresql, but maybe not for oracle?
func toLower(input []string) []string {
	output := make([]string, len(input))
	for i, str := range input {
		output[i] = strings.ToLower(str)
	}
	return output
}

// TODO: interface???
func (config *Config) toKDVH() *kdvh.DumpConfig {
	return &kdvh.DumpConfig{
		Tables:    config.Tables,
		Stations:  config.Stations,
		Elements:  config.Elements,
		BaseDir:   config.BaseDir,
		Email:     config.Email,
		Overwrite: config.Overwrite,
	}
}

// TODO: this should be in main?
func (config *Config) Execute(_ []string) error {
	config.setup()

	if config.System == "kdvh" || config.System == "all" {
		kdvh.Init().Dump(config.toKDVH())
	}

	if config.System == "kvalobs" || config.System == "all" {
		// kvalobs.Init().Dump(config.toKvalobs())
	}

	return nil
}

// TODO: This is only useful if the different tables are defined as separate structs?
// type Dumper interface {
// 	Dump(conn *sql.DB, config *DumpConfig)
// }
