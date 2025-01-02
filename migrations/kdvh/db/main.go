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

// TODO: there other tables in the proxy, should they also be dumped?
// T_DIURNAL_TJ
// T_FDATA
// T_GRID_DATA
// T_LAUV_SPRETT
// T_NORMAL_DIURNAL
// T_NORMAL_MONTH
// T_ORIGINALDATA
// T_ORIGINALDATA_SVV
// T_RECORDS
// T_REGION
// T_RR_INTENSITY
// T_RR_RETURNPERIOD
// T_SEASON
// T_WLF_DATA
//
// TODO: not sure a single elem table lists all timeseries for a given table
// t_elem_normal_diurnal
// t_elem_normal_month
// t_elem_diurnal (T_DIURNAL, T_DIURNAL_TJ, T_MINUTE_DATA)
// t_elem_edata (T_CCDV_DATA, T_DIURNAL, T_EDATA, T_MERMAID)
// t_elem_fdata (T_FDATA, T_METARDATA)
// t_elem_homogen_month (T_HOMOGEN_MONTH, T_HOMOGEN_DIURNAL)
// t_elem_map_timeseries -> (232638, all timeseries? map to what?)
// (T_10MINUTE_DATA, T_ADATA, T_ADATA_LEVEL, T_AVINOR, T_CDCV_DATA, T_DIURNAL, T_DIURNAL_TJ, T_EDATA, T_GRID_DATA, T_HOMOGEN_DIURNAL, T_HOMOGEN_MONTH, T_LAUV_SPRETT, T_MDATA, T_MERMAID, T_METARDATA, T_MINUTE_DATA, T_MONTH, T_NDATA, T_PDATA, T_SEASON, T_SECOND_DATA, T_SVVDATA, T_TJ_DATA, T_UTLANDDATA, T_VDATA, T_WLF_DATA)
// t_elem_month (T_MONTH)
// t_elem_obs ->
// (T_10MINUTE_DATA, T_ADATA, T_ADATA_LEVEL, T_AVINOR, T_DIURNAL, T_LAUV_SPRETT, T_MDATA, T_MINUTE_DATA, T_NDATA, T_PDATA, T_SECOND_DATA, T_SVVDATA, T_TJ_DATA, T_UTLANDDATA, T_VDATA, T_WLF_DATA)
// t_elem_pdata (T_PDATA)
// t_elem_proj (T_AVINOR, T_PROJDATA)
// t_elem_season (T_SEASON)
func Init() *KDVH {
	return &KDVH{map[string]*Table{
		// Section 1: tables that need to be migrated entirely
		"T_EDATA":     NewTable("T_EDATA", "T_EFLAG", "T_ELEM_EDATA").SetConvertFunc(convertEdata).SetImportYear(3000),
		"T_METARDATA": NewTable("T_METARDATA", "", "T_ELEM_FDATA").SetDumpFunc(dumpDataOnly).SetImportYear(3000),

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
		"T_AVINOR":        NewTable("T_AVINOR", "T_AVINOR_FLAG", "T_ELEM_OBS"),

		// Section 4: special cases, namely digitized historical data
		// NOTE: I don't think we want to import these, they are products
		"T_MONTH":           NewTable("T_MONTH", "T_MONTH_FLAG", "T_ELEM_MONTH").SetConvertFunc(convertProduct).SetImportYear(1957),
		"T_DIURNAL":         NewTable("T_DIURNAL", "T_DIURNAL_FLAG", "T_ELEM_DIURNAL").SetConvertFunc(convertProduct).SetImportYear(2006),
		"T_HOMOGEN_DIURNAL": NewTable("T_HOMOGEN_DIURNAL", "", "T_ELEM_HOMOGEN_MONTH").SetDumpFunc(dumpDataOnly).SetConvertFunc(convertProduct),
		"T_HOMOGEN_MONTH":   NewTable("T_HOMOGEN_MONTH", "", "T_ELEM_HOMOGEN_MONTH").SetDumpFunc(dumpHomogenMonth).SetConvertFunc(convertProduct),

		// Section 5: tables missing in the KDVH proxy:
		// 1. this one exists in a separate database
		// "T_PROJDATA": NewTable("T_PROJDATA", "T_PROJFLAG", "T_ELEM_PROJ"),
		// 2. these are not in active use and don't need to be imported in LARD
		// "T_DIURNAL_INTERPOLATED": NewTable("T_DIURNAL_INTERPOLATED", "", "T_ELEM_DIURNAL").SetConvertFunc(convertDiurnalInterpolated),
		// "T_MONTH_INTERPOLATED":   NewTable("T_MONTH_INTERPOLATED", "", "T_ELEM_MONTH"),
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
