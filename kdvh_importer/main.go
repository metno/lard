package main

import (
	"fmt"
	"log"

	// TODO: might move to go-arg, seems nicer
	"github.com/jessevdk/go-flags"
	"github.com/joho/godotenv"
)

// DataPageFunction is a function that creates a properly formatted TimeSeriesData object
type DataPageFunction func(ObsKDVH) (ObsLARD, error)

// TableInstructions contain metadata on how to treat different tables in KDVH
type TableInstructions struct {
	TableName     string           // Name of the table with observations
	FlagTableName string           // Name of the table with QC flags for observations
	ElemTableName string           // Frost proxy table storing KDVH elements (?)
	DataFunction  DataPageFunction // Converter from KDVH obs to LARD obs
	ImportUntil   int              // stop when reaching this year
	FromKlima11   bool             // dump from klima11 not dvh10
	SplitQuery    bool             // split dump queries into yearly batches
}

// List of all the tables we care about
var TABLE2INSTRUCTIONS = map[string]*TableInstructions{
	// unique tables imported in their entirety
	"T_EDATA":     {TableName: "T_EDATA", FlagTableName: "T_EFLAG", ElemTableName: "T_ELEM_EDATA", DataFunction: makeDataPageEdata, ImportUntil: 3001},
	"T_METARDATA": {TableName: "T_METARDATA", ElemTableName: "T_ELEM_METARDATA", DataFunction: makeDataPage, ImportUntil: 3000},

	// TODO: these two are the only tables seemingly missing from the KDVH proxy
	// "T_DIURNAL_INTERPOLATED": {TableName: "T_DIURNAL_INTERPOLATED", DataFunction: makeDataPageDiurnalInterpolated, ImportUntil: 3000},
	// "T_MONTH_INTERPOLATED":   {TableName: "T_MONTH_INTERPOLATED", DataFunction: makeDataPageDiurnalInterpolated, ImportUntil: 3000},

	// tables with some data in kvalobs, import only up to 2005-12-31
	"T_ADATA":      {TableName: "T_ADATA", FlagTableName: "T_AFLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPage, ImportUntil: 2006},
	"T_MDATA":      {TableName: "T_MDATA", FlagTableName: "T_MFLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPage, ImportUntil: 2006},
	"T_TJ_DATA":    {TableName: "T_TJ_DATA", FlagTableName: "T_TJ_FLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPage, ImportUntil: 2006},
	"T_PDATA":      {TableName: "T_PDATA", FlagTableName: "T_PFLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPagePdata, ImportUntil: 2006},
	"T_NDATA":      {TableName: "T_NDATA", FlagTableName: "T_NFLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPageNdata, ImportUntil: 2006},
	"T_VDATA":      {TableName: "T_VDATA", FlagTableName: "T_VFLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPageVdata, ImportUntil: 2006},
	"T_UTLANDDATA": {TableName: "T_UTLANDDATA", FlagTableName: "T_UTLANDFLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPage, ImportUntil: 2006},
	// tables that should only be dumped
	"T_10MINUTE_DATA": {TableName: "T_10MINUTE_DATA", FlagTableName: "T_10MINUTE_FLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPage, SplitQuery: true},
	"T_MINUTE_DATA":   {TableName: "T_MINUTE_DATA", FlagTableName: "T_MINUTE_FLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPage, SplitQuery: true},
	"T_SECOND_DATA":   {TableName: "T_SECOND_DATA", FlagTableName: "T_SECOND_FLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPage, SplitQuery: true},
	"T_ADATA_LEVEL":   {TableName: "T_ADATA_LEVEL", FlagTableName: "T_AFLAG_LEVEL", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPage},
	"T_CDCV_DATA":     {TableName: "T_CDCV_DATA", FlagTableName: "T_CDCV_FLAG", ElemTableName: "T_ELEM_EDATA", DataFunction: makeDataPage},
	"T_MERMAID":       {TableName: "T_MERMAID", FlagTableName: "T_MERMAID_FLAG", ElemTableName: "T_ELEM_EDATA", DataFunction: makeDataPage},
	"T_DIURNAL":       {TableName: "T_DIURNAL", FlagTableName: "T_DIURNAL_FLAG", ElemTableName: "T_ELEM_DIURNAL", DataFunction: makeDataPageProduct, ImportUntil: 2006},
	"T_SVVDATA":       {TableName: "T_SVVDATA", FlagTableName: "T_SVVFLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPage},
	"T_AVINOR":        {TableName: "T_AVINOR", FlagTableName: "T_AVINOR_FLAG", ElemTableName: "T_ELEM_OBS", DataFunction: makeDataPage, FromKlima11: true},
	"T_PROJDATA":      {TableName: "T_PROJDATA", FlagTableName: "T_PROJFLAG", ElemTableName: "T_ELEM_PROJ", DataFunction: makeDataPage, FromKlima11: true},
	// other special cases
	"T_MONTH":           {TableName: "T_MONTH", FlagTableName: "T_MONTH_FLAG", ElemTableName: "T_ELEM_MONTH", DataFunction: makeDataPageProduct, ImportUntil: 1957},
	"T_HOMOGEN_DIURNAL": {TableName: "T_HOMOGEN_DIURNAL", ElemTableName: "T_ELEM_HOMOGEN_MONTH", DataFunction: makeDataPageProduct},
	"T_HOMOGEN_MONTH":   {TableName: "T_HOMOGEN_MONTH", ElemTableName: "T_ELEM_HOMOGEN_MONTH", DataFunction: makeDataPageProduct},
	// metadata notes for other tables
	// "T_SEASON": {ElemTableName: "T_SEASON"},
}

type CmdArgs struct {
	// TODO: These might need to be implemented later, right now we are only focusing on importing already dumped tables
	// SwitchTableType    string `long:"switch" choice:"default" choice:"fetchkdvh" description:"perform source switch, can be 'default' or 'fetchkdvh'"`
	// SwitchWholeTable   bool   `long:"switchtable" description:"source switch all timeseries – if defined together with switch, this will switch all timeseries in table, not just those found in datadir"`
	// SwitchAll          bool   `long:"switchall" description:"source switch all timeseries – if given together with switch, this will run a type switch for all timeseries of all data tables that have a combined folder"`
	// Validate           bool   `long:"validate" description:"perform data validation – if given, imported data will be validated against KDVH"`
	// ValidateAll        bool   `long:"validateall" description:"validate all timeseries – if defined, this will run validation for all data tables that have a combined folder"`
	// ValidateWholeTable bool   `long:"validatetable" description:"validate all timeseries – if defined together with validate, this will compare ODA with all KDVH timeseries, not just those found in datadir"`
	Import ImportArgs `command:"import" description:"Import dumped tables"`
	Dump   DumpArgs   `command:"dump" description:"Dump tables"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

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
