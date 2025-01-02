package db

import (
	"migrate/stinfosys"
	"migrate/utils"
	"time"

	"github.com/rickb777/period"
)

const KDVH_ENV_VAR string = "KDVH_PROXY_CONN_STRING"

// Map of all tables found in KDVH, with set max import year
type KDVH struct {
	Tables map[string]*Table
}

func Init() *KDVH {
	// TODO: not sure a single elem table lists all timeseries for a given table,
	// for example "T_CCDV_DATA" is in both "T_ELEM_EDATA" and "T_ELEM_DIURNAL"
	return &KDVH{map[string]*Table{
		// Section 1: tables that need to be migrated entirely
		// TODO: figure out if we need to use the elem_code_paramid_level_sensor_t_edata table?
		"T_EDATA": NewTable("T_EDATA", "T_EFLAG", "T_ELEM_EDATA").SetConvertFunc(convertEdata).SetImportYear(3000),
		// FIXME: T_ELEM_METARDATA not in proxy
		"T_METARDATA": NewTable("T_METARDATA", "", "").SetDumpFunc(dumpDataOnly).SetImportYear(3000),

		// Section 2: tables with some data in kvalobs, import only up to 2005-12-31
		"T_ADATA":      NewTable("T_ADATA", "T_AFLAG", "T_ELEM_OBS").SetImportYear(2006),
		"T_MDATA":      NewTable("T_MDATA", "T_MFLAG", "T_ELEM_OBS").SetImportYear(2006),
		"T_TJ_DATA":    NewTable("T_TJ_DATA", "T_TJ_FLAG", "T_ELEM_OBS").SetImportYear(2006),
		"T_PDATA":      NewTable("T_PDATA", "T_PFLAG", "T_ELEM_OBS").SetConvertFunc(convertPdata).SetImportYear(2006),
		"T_NDATA":      NewTable("T_NDATA", "T_NFLAG", "T_ELEM_OBS").SetConvertFunc(convertNdata).SetImportYear(2006),
		"T_VDATA":      NewTable("T_VDATA", "T_VFLAG", "T_ELEM_OBS").SetConvertFunc(convertVdata).SetImportYear(2006),
		"T_UTLANDDATA": NewTable("T_UTLANDDATA", "T_UTLANDFLAG", "T_ELEM_OBS").SetImportYear(2006),

		// Section 3: tables that should only be dumped
		"T_10MINUTE_DATA": NewTable("T_10MINUTE_DATA", "T_10MINUTE_FLAG", "T_ELEM_OBS"),
		"T_ADATA_LEVEL":   NewTable("T_ADATA_LEVEL", "T_AFLAG_LEVEL", "T_ELEM_OBS"),
		"T_MINUTE_DATA":   NewTable("T_MINUTE_DATA", "T_MINUTE_FLAG", "T_ELEM_OBS"),
		"T_SECOND_DATA":   NewTable("T_SECOND_DATA", "T_SECOND_FLAG", "T_ELEM_OBS"),
		"T_CDCV_DATA":     NewTable("T_CDCV_DATA", "T_CDCV_FLAG", "T_ELEM_EDATA"),
		"T_MERMAID":       NewTable("T_MERMAID", "T_MERMAID_FLAG", "T_ELEM_EDATA"),
		"T_SVVDATA":       NewTable("T_SVVDATA", "T_SVVFLAG", "T_ELEM_OBS"),

		// Section 4: special cases, namely digitized historical data
		// TODO: I don't think we want to import these, they are products
		"T_MONTH":           NewTable("T_MONTH", "T_MONTH_FLAG", "T_ELEM_MONTH").SetConvertFunc(convertProduct).SetImportYear(1957),
		"T_DIURNAL":         NewTable("T_DIURNAL", "T_DIURNAL_FLAG", "T_ELEM_DIURNAL").SetConvertFunc(convertProduct).SetImportYear(2006),
		"T_HOMOGEN_DIURNAL": NewTable("T_HOMOGEN_DIURNAL", "", "T_ELEM_HOMOGEN_MONTH").SetDumpFunc(dumpDataOnly).SetConvertFunc(convertProduct),
		"T_HOMOGEN_MONTH":   NewTable("T_HOMOGEN_MONTH", "", "T_ELEM_HOMOGEN_MONTH").SetDumpFunc(dumpHomogenMonth).SetConvertFunc(convertProduct),

		// Section 5: tables missing in the KDVH proxy:
		// 1. these exist in a separate database
		"T_AVINOR":   NewTable("T_AVINOR", "T_AVINOR_FLAG", "T_ELEM_OBS"),
		"T_PROJDATA": NewTable("T_PROJDATA", "T_PROJFLAG", "T_ELEM_PROJ"),
		// 2. these are not in active use and don't need to be imported in LARD
		// TODO: are these the correct elem tables?
		// FIXME: these are not accessible through the proxy
		"T_DIURNAL_INTERPOLATED": NewTable("T_DIURNAL_INTERPOLATED", "", "T_ELEM_DIURNAL").SetConvertFunc(convertDiurnalInterpolated),
		"T_MONTH_INTERPOLATED":   NewTable("T_MONTH_INTERPOLATED", "", "T_ELEM_MONTH"),
	}}
}

// Struct that represent an observation in KDVH
type KdvhObs struct {
	Obstime time.Time
	Data    string
	Flags   string
}

// Convenience struct that holds information for a specific timeseries
type TsInfo struct {
	Id       int64
	Station  int32
	Element  string
	Offset   period.Period
	Param    stinfosys.Param
	Timespan utils.TimeSpan
	Logstr   string
}
