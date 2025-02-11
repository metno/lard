package db

import (
	"migrate/utils"
	"time"

	"github.com/rickb777/period"
)

// In KDVH for each table name we usually have three separate tables:
// 1. A DATA table containing observation values;
// 2. A FLAG table containing quality control (QC) flags;
// 3. A ELEM table containing metadata about the validity of the timeseries.
//
// DATA and FLAG tables have the same schema:
// | dato | stnr | ... |
// where 'dato' is the timestamp of the observation, 'stnr' is the station
// where the observation was measured, and '...' is a varying number of columns
// each with different observations, where the column name is the 'elem_code'
// (e.g. for air temperature, 'ta').
//
// The ELEM tables have the following schema:
// | stnr | elem_code | fdato | tdato | table_name | flag_table_name | audit_dato

const KDVH_ENV_VAR string = "KDVH_PROXY_CONN_STRING"

// Struct that represent an observation in KDVH
type Obs struct {
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
	IsScalar bool
	Timespan utils.TimeSpan
	Logstr   string
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
// t_elem_map_timeseries (232638, all timeseries? map to what?) ->
// (T_10MINUTE_DATA, T_ADATA, T_ADATA_LEVEL, T_AVINOR, T_CDCV_DATA, T_DIURNAL, T_DIURNAL_TJ, T_EDATA, T_GRID_DATA, T_HOMOGEN_DIURNAL, T_HOMOGEN_MONTH, T_LAUV_SPRETT, T_MDATA, T_MERMAID, T_METARDATA, T_MINUTE_DATA, T_MONTH, T_NDATA, T_PDATA, T_SEASON, T_SECOND_DATA, T_SVVDATA, T_TJ_DATA, T_UTLANDDATA, T_VDATA, T_WLF_DATA)
// t_elem_month (T_MONTH)
// t_elem_obs (89367) ->
// (T_10MINUTE_DATA, T_ADATA, T_ADATA_LEVEL, T_AVINOR, T_DIURNAL, T_LAUV_SPRETT, T_MDATA, T_MINUTE_DATA, T_NDATA, T_PDATA, T_SECOND_DATA, T_SVVDATA, T_TJ_DATA, T_UTLANDDATA, T_VDATA, T_WLF_DATA)
// t_elem_pdata (T_PDATA)
// t_elem_proj (T_AVINOR, T_PROJDATA)
// t_elem_season (T_SEASON)
