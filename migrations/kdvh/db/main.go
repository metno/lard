package db

import (
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
	Logstr   string
}
