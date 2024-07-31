package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

type DumpArgs struct {
	BaseDir      string `long:"dir" required:"true" description:"Base directory where the dumped data is stored"`
	Sep          string `long:"sep" default:";"  description:"Separator character in the dumped files"`
	TableList    string `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationList  string `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElemCodeList string `long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	SkipData     bool   `long:"skipdata" description:"If given, the values from dataTable will NOT be processed"`
	SkipFlags    bool   `long:"skipflag" description:"If given, the values from flagTable will NOT be processed"`
	Limit        string `long:"limit" default:"" description:"If given, the procedure will stop after migrating this number of stations"`
	Overwrite    bool   `long:"overwrite" description:"Overwrite any existing dumped files"`
	// Tables       []string
	// Stations     []string
	// Elements     []string
	// OffsetMap    map[ParamKey]period.Period
	// MetadataMap  map[ParamKey]Metadata
}

func (self *DumpArgs) Execute(args []string) error {
	fmt.Println("Subcommand 'dump' is not currently implemented")
	return nil
}

// TODO: try to convert this bash stuff to go (using godror?)
func dumpData(table *TableInstructions, args *DumpArgs) {
	// defer EmailOnPanic("dumpData")

	// convert golang boolean to string boolean for bash
	FromKlima11 := "0"
	if table.FromKlima11 {
		FromKlima11 = "1"
	}
	SplitQuery := "0"
	if table.SplitQuery {
		SplitQuery = "1"
	}

	stnrArg := "0"
	if args.StationList != "" {
		stnrArg = args.StationList
	}

	elemCodearg := "0"
	if args.ElemCodeList != "" {
		elemCodearg = args.ElemCodeList
	}

	// trigger shell script to dump dataTable
	if !args.SkipData {
		combDir := args.BaseDir + table.TableName
		if _, err := os.ReadDir(combDir); err == nil && !args.Overwrite {
			log.Println("Skipping data dump of", table.TableName, "because dumped folder already exists")
		} else {
			log.Println("Starting data dump of", table.TableName)
			setLogFile(table.TableName, "dump")
			out, err := exec.Command(
				"./dataandflags_dump.sh",
				args.BaseDir,
				table.TableName,
				args.Limit,
				FromKlima11,
				SplitQuery,
				"0",
				stnrArg,
				elemCodearg,
			).Output()
			if err != nil {
				log.Println(string(out))
				log.Panicln(fmt.Sprintf("Shell script failed on data dump - %s", err))
			}
			log.Println(string(out))
			log.SetOutput(os.Stdout)
			log.Println("Finished data dump of", table.TableName)
		}
	}

	// trigger shell script to dump flagTable
	if !args.SkipFlags && table.FlagTableName != "" {
		combDir := args.BaseDir + table.FlagTableName
		if _, err := os.ReadDir(combDir); err == nil && !args.Overwrite {
			log.Println("Skipping flag dump of", table.FlagTableName, "because dumped folder already exists")
		} else {
			log.Println("Starting flag dump of", table.FlagTableName)
			setLogFile(table.TableName, "dump")
			out, err := exec.Command(
				"./dataandflags_dump.sh",
				args.BaseDir,
				table.FlagTableName,
				args.Limit,
				FromKlima11,
				SplitQuery,
				"0",
				stnrArg,
				elemCodearg,
			).Output()
			if err != nil {
				log.Println(string(out))
				log.Panicln(fmt.Sprintf("Shell script failed on flag dump - %s", err))
			}
			log.Println(string(out))
			log.SetOutput(os.Stdout)
			log.Println("Finished flag dump of", table.FlagTableName)
		}
	}
}
