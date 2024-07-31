package main

import (
	"fmt"

	// TODO: might move to go-arg, seems nicer
	"github.com/jessevdk/go-flags"
)

type CmdArgs struct {
	BaseDir      string `long:"dir" default:"./" description:"Base directory where the dumped data is stored"`
	Sep          string `long:"sep" default:";"  description:"Separator character in the dumped files"`
	TableList    string `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationList  string `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElemCodeList string `long:"elemcode" default:"" description:"Optional comma separated list of element codes.  By default all element codes are processed"`
	Import       bool   `long:"import" description:"Import the given combined data"`
	Email        string `long:"email" default:"" description:"Optional email address used to notify if the program crashed"`

	// TODO: These might need to be implemented later, right now we are only focusing on importing already dumped tables
	// ImportAll    bool   `long:"importall" description:"Import all combined table directories"`
	// Dump               bool   `long:"dump" description:"(optional, for method 'data') – if given, data will be dumped from KDVH"`
	// DumpAll            bool   `long:"dumpall" description:"(optional, for method 'data') – if given, data will be dumped from KDVH, performed for all tables missing a folder"`
	// SkipData           bool   `long:"skipdatadump" description:"skip data table – if given, the values from dataTable will NOT be processed"`
	// SkipFlags          bool   `long:"skipflagdump" description:"skip flag table – if given, the values from flagTable will NOT be processed"`
	// Limit              string `long:"limit" default:"" description:"limit – if given, the procedure will stop after migrating this number of stations"`
	// SwitchTableType    string `long:"switch" choice:"default" choice:"fetchkdvh" description:"perform source switch, can be 'default' or 'fetchkdvh'"`
	// SwitchWholeTable   bool   `long:"switchtable" description:"source switch all timeseries – if defined together with switch, this will switch all timeseries in table, not just those found in datadir"`
	// SwitchAll          bool   `long:"switchall" description:"source switch all timeseries – if given together with switch, this will run a type switch for all timeseries of all data tables that have a combined folder"`
	// Validate           bool   `long:"validate" description:"perform data validation – if given, imported data will be validated against KDVH"`
	// ValidateAll        bool   `long:"validateall" description:"validate all timeseries – if defined, this will run validation for all data tables that have a combined folder"`
	// ValidateWholeTable bool   `long:"validatetable" description:"validate all timeseries – if defined together with validate, this will compare ODA with all KDVH timeseries, not just those found in datadir"`
	// Overwrite          bool   `long:"overwrite" description:"overwrite files – if given, then any existing dumped files will be overwritten"`

	// TODO:Can also use subcommands
	// DataCmd struct {
	//    ...
	// } `command:"data"`
	//
	// NormalsCmd struct {
	//    ...
	// } `command:"normals"`
	//
	// or:
	// DumpCmd struct {
	//    ...
	// } `command:"dump"`
	//
	// or:
	// ImportCmd struct {
	//    ...
	// } `command:"import"`
}

func processArgs() (*CmdArgs, error) {
	args := CmdArgs{}
	_, err := flags.Parse(&args)

	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok {
			if flagsErr.Type == flags.ErrHelp {
				return nil, err
			}
		}
		fmt.Println("Type 'kdvh_importer -h' for help")
		return nil, err
	}

	return &args, nil
}
