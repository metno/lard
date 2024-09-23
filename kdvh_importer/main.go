package main

import (
	"fmt"
	"log"

	"github.com/jessevdk/go-flags"
	"github.com/joho/godotenv"
)

// TableInstructions contain metadata on how to treat different tables in KDVH
type TableInstructions struct {
	TableName     string            // Name of the table with observations
	FlagTableName string            // Name of the table with QC flags for observations
	ElemTableName string            // Name of table storing metadata
	ImportUntil   int               // stop when reaching this year
	FromKlima11   bool              // dump from klima11 not dvh10
	ConvFunc      ConvertFunction   // Converter from KDVH obs to LARD obs
	DumpFunc      TableDumpFunction //
}

func (table *TableInstructions) updateDefaults() {
	if table.ConvFunc == nil {
		table.ConvFunc = makeDataPage
	}
	if table.DumpFunc == nil {
		if table.FlagTableName == "" {
			table.DumpFunc = dumpDataOnly
		} else {
			table.DumpFunc = dumpDataAndFlags
		}
	}
}

// List of all the tables we care about
var TABLE2INSTRUCTIONS = []*TableInstructions{
	// Section 1: unique tables imported in their entirety
	{TableName: "T_EDATA", FlagTableName: "T_EFLAG", ElemTableName: "T_ELEM_EDATA", ConvFunc: makeDataPageEdata, ImportUntil: 3000},
	// all tables below are dumped
	{TableName: "T_METARDATA", ElemTableName: "T_ELEM_METARDATA", ImportUntil: 3000},

	// TODO: these two are the only tables seemingly missing from the KDVH proxy
	// {TableName: "T_DIURNAL_INTERPOLATED", DataFunction: makeDataPageDiurnalInterpolated, ImportUntil: 3000},
	// {TableName: "T_MONTH_INTERPOLATED", DataFunction: makeDataPageDiurnalInterpolated, ImportUntil: 3000},

	// Section 2: tables with some data in kvalobs, import only up to 2005-12-31
	{TableName: "T_ADATA", FlagTableName: "T_AFLAG", ElemTableName: "T_ELEM_OBS", ImportUntil: 2006},
	// all tables below are dumped
	{TableName: "T_MDATA", FlagTableName: "T_MFLAG", ElemTableName: "T_ELEM_OBS", ImportUntil: 2006},
	{TableName: "T_TJ_DATA", FlagTableName: "T_TJ_FLAG", ElemTableName: "T_ELEM_OBS", ImportUntil: 2006},
	{TableName: "T_PDATA", FlagTableName: "T_PFLAG", ElemTableName: "T_ELEM_OBS", ConvFunc: makeDataPagePdata, ImportUntil: 2006},
	{TableName: "T_NDATA", FlagTableName: "T_NFLAG", ElemTableName: "T_ELEM_OBS", ConvFunc: makeDataPageNdata, ImportUntil: 2006},
	{TableName: "T_VDATA", FlagTableName: "T_VFLAG", ElemTableName: "T_ELEM_OBS", ConvFunc: makeDataPageVdata, ImportUntil: 2006},
	{TableName: "T_UTLANDDATA", FlagTableName: "T_UTLANDFLAG", ElemTableName: "T_ELEM_OBS", ImportUntil: 2006},

	// Section 3: tables that should only be dumped
	{TableName: "T_10MINUTE_DATA", FlagTableName: "T_10MINUTE_FLAG", ElemTableName: "T_ELEM_OBS", DumpFunc: dumpByYear},
	{TableName: "T_ADATA_LEVEL", FlagTableName: "T_AFLAG_LEVEL", ElemTableName: "T_ELEM_OBS"},
	{TableName: "T_DIURNAL", FlagTableName: "T_DIURNAL_FLAG", ElemTableName: "T_ELEM_DIURNAL", ConvFunc: makeDataPageProduct},
	{TableName: "T_AVINOR", FlagTableName: "T_AVINOR_FLAG", ElemTableName: "T_ELEM_OBS", FromKlima11: true},
	{TableName: "T_PROJDATA", FlagTableName: "T_PROJFLAG", ElemTableName: "T_ELEM_PROJ", FromKlima11: true},
	// all tables below are dumped
	{TableName: "T_MINUTE_DATA", FlagTableName: "T_MINUTE_FLAG", ElemTableName: "T_ELEM_OBS", DumpFunc: dumpByYear},
	{TableName: "T_SECOND_DATA", FlagTableName: "T_SECOND_FLAG", ElemTableName: "T_ELEM_OBS", DumpFunc: dumpByYear},
	{TableName: "T_CDCV_DATA", FlagTableName: "T_CDCV_FLAG", ElemTableName: "T_ELEM_EDATA"},
	{TableName: "T_MERMAID", FlagTableName: "T_MERMAID_FLAG", ElemTableName: "T_ELEM_EDATA"},
	{TableName: "T_SVVDATA", FlagTableName: "T_SVVFLAG", ElemTableName: "T_ELEM_OBS"},

	// Section 4: other special cases
	{TableName: "T_MONTH", FlagTableName: "T_MONTH_FLAG", ElemTableName: "T_ELEM_MONTH", ConvFunc: makeDataPageProduct, ImportUntil: 1957},
	{TableName: "T_HOMOGEN_DIURNAL", ElemTableName: "T_ELEM_HOMOGEN_MONTH", ConvFunc: makeDataPageProduct},
	{TableName: "T_HOMOGEN_MONTH", ElemTableName: "T_ELEM_HOMOGEN_MONTH", ConvFunc: makeDataPageProduct, DumpFunc: dumpHomogenMonth},

	// metadata notes for other tables
	// {ElemTableName: "T_SEASON"},
}

type CmdArgs struct {
	// TODO: These might need to be implemented later
	// SwitchTableType    string `long:"switch" choice:"default" choice:"fetchkdvh" description:"perform source switch, can be 'default' or 'fetchkdvh'"`
	// SwitchWholeTable   bool   `long:"switchtable" description:"source switch all timeseries – if defined together with switch, this will switch all timeseries in table, not just those found in datadir"`
	// SwitchAll          bool   `long:"switchall" description:"source switch all timeseries – if given together with switch, this will run a type switch for all timeseries of all data tables that have a combined folder"`
	// Validate           bool   `long:"validate" description:"perform data validation – if given, imported data will be validated against KDVH"`
	// ValidateAll        bool   `long:"validateall" description:"validate all timeseries – if defined, this will run validation for all data tables that have a combined folder"`
	// ValidateWholeTable bool   `long:"validatetable" description:"validate all timeseries – if defined together with validate, this will compare ODA with all KDVH timeseries, not just those found in datadir"`
	List   ListConfig   `command:"tables" description:"List available tables"`
	Dump   DumpConfig   `command:"dump" description:"Dump tables from KDVH to CSV"`
	Import ImportConfig `command:"import" description:"Import dumped CSV files"`
}

type ListConfig struct{}

func (config *ListConfig) Execute(_ []string) error {
	fmt.Println("Available tables:")
	for _, table := range TABLE2INSTRUCTIONS {
		fmt.Println("    -", table.TableName)
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Loads "LARD_STRING", "STINFO_STRING","KDVH_PROXY_CONN"
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
