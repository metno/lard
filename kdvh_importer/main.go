package main

import (
	"fmt"
	"log"

	"kdvh_importer/dump"
	"kdvh_importer/migrate"

	"github.com/jessevdk/go-flags"
	"github.com/joho/godotenv"
)

type CmdArgs struct {
	// TODO: These might need to be implemented at a later time
	// SwitchTableType    string `long:"switch" choice:"default" choice:"fetchkdvh" description:"perform source switch, can be 'default' or 'fetchkdvh'"`
	// SwitchWholeTable   bool   `long:"switchtable" description:"source switch all timeseries – if defined together with switch, this will switch all timeseries in table, not just those found in datadir"`
	// SwitchAll          bool   `long:"switchall" description:"source switch all timeseries – if given together with switch, this will run a type switch for all timeseries of all data tables that have a combined folder"`
	// Validate           bool   `long:"validate" description:"perform data validation – if given, imported data will be validated against KDVH"`
	// ValidateAll        bool   `long:"validateall" description:"validate all timeseries – if defined, this will run validation for all data tables that have a combined folder"`
	// ValidateWholeTable bool   `long:"validatetable" description:"validate all timeseries – if defined together with validate, this will compare ODA with all KDVH timeseries, not just those found in datadir"`
	List   ListConfig     `command:"tables" description:"List available tables"`
	Dump   dump.Config    `command:"dump" description:"Dump tables from KDVH to CSV"`
	Import migrate.Config `command:"import" description:"Import dumped CSV files"`
}

type ListConfig struct{}

func (config *ListConfig) Execute(_ []string) error {
	fmt.Println("Available tables:")
	// TODO: refactor
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Need the following env variables:
	// 1. Dump
	//    - kdvh: "KDVH_PROXY_CONN"
	// 2. Import
	//    - kdvh: "LARD_STRING", "STINFO_STRING", "KDVH_PROXY_CONN"
	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	// NOTE: go-flags calls the Execute method on the parsed subcommand
	_, err = flags.Parse(&CmdArgs{})
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok {
			if flagsErr.Type == flags.ErrHelp {
				return
			}
		}
		fmt.Println("Type 'kdvh_importer -h' for help")
		return
	}
}
