package dump

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DumpFunction func(path, element, station, dataTable, flagTable string, pool *pgxpool.Pool) error
type Table struct {
	TableName     string // Name of the DATA table
	FlagTableName string // Name of the FLAG table
	ElemTableName string // Name of the ELEM table
	Query         string // Query used to dump from the table
	// The query is actually a format string since we need to dump columns separatetly and the columns are fetched dynamically
}

func NewTable(data, flag, elem string) *Table {
	var query string

	// Set the query string
	switch data {
	case "T_HOMOGEN_MONTH":
		// T_HOMOGEN_MONTH contains seasonal and annual data, plus other derivative
		// data combining both of these.
		// We decided to dump only the monthly data (season BETWEEN 1 AND 12) for
		//   - TAM (mean hourly temperature), and
		//   - RR (hourly precipitations, note that in Stinfosys this parameter is 'RR_1')
		//
		// We plan to calculate the other data on the fly (in the egress) if needed.
		query =
			fmt.Sprintf(`
			SELECT
				dato AS time,
				'' AS typeid,
				%%[1]s AS data,
				'' AS flag
			FROM %s
			WHERE %%[1]s IS NOT NULL
				AND stnr = $1
				AND season BETWEEN 1 AND 12
				AND dato BETWEEN $2 AND $3`,
				data)

	case "T_METARDATA":
		// Missing Flag table
		query = fmt.Sprintf(`
			SELECT
				dato AS time,
				typeid,
				%%[1]s AS data,
				'' AS flag
			FROM %s
			WHERE %%[1]s IS NOT NULL
			AND stnr = $1
			AND dato BETWEEN $2 AND $3`,
			data)

	case "T_DIURNAL", "T_MONTH":
		// Missing typeid column
		query = fmt.Sprintf(`
		SELECT
			dato AS time,
			'' AS typeid,
			d.%%[1]s AS data,
			f.%%[1]s AS flag
		FROM (
			SELECT dato, %%[1]s FROM %s
				WHERE %%[1]s IS NOT NULL
				AND stnr = $1
				AND dato BETWEEN $2 AND $3
		) d FULL OUTER JOIN (
			SELECT dato, %%[1]s FROM %s
				WHERE %%[1]s IS NOT NULL
				AND stnr = $1
				AND dato BETWEEN $2 AND $3
		) f USING(dato)`,
			data, flag)

	// TODO: maybe merge with T_METARDATA and a COALESCE on typeid?
	case "T_HOMOGEN_DIURNAL":
		// Missing Flag table and typeid column
		query = fmt.Sprintf(`
		SELECT
			dato AS time,
			'' AS typeid,
			%%[1]s AS data,
			'' AS flag
		FROM %s
		WHERE %[1]s IS NOT NULL
		AND stnr = $1
		AND dato BETWEEN $2 AND $3`, data)

	default:
		query = fmt.Sprintf(`
		SELECT
			dato AS time,
			COALESCE(d.typeid::text, f.typeid::text) AS typeid,
			d.%%[1]s AS data, f.%%[1]s AS flag
		FROM (
			SELECT dato, typeid, %%[1]s FROM %s
				WHERE %%[1]s IS NOT NULL
				AND stnr = $1
				AND dato BETWEEN $2 AND $3
		) d FULL OUTER JOIN (
			SELECT dato, typeid, %%[1]s FROM %s
				WHERE %%[1]s IS NOT NULL
				AND stnr = $1
				AND dato BETWEEN $2 AND $3
		) f USING(dato)`,
			data, flag)
	}

	return &Table{
		TableName:     data,
		FlagTableName: flag,
		ElemTableName: elem,
		Query:         query,
	}
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
func InitDumpTables() []*Table {
	return []*Table{
		NewTable("T_EDATA", "T_EFLAG", "T_ELEM_EDATA"),
		NewTable("T_METARDATA", "", "T_ELEM_FDATA"),

		NewTable("T_ADATA", "T_AFLAG", "T_ELEM_OBS"),
		NewTable("T_MDATA", "T_MFLAG", "T_ELEM_OBS"),
		NewTable("T_TJ_DATA", "T_TJ_FLAG", "T_ELEM_OBS"),
		NewTable("T_PDATA", "T_PFLAG", "T_ELEM_OBS"),
		NewTable("T_NDATA", "T_NFLAG", "T_ELEM_OBS"),
		NewTable("T_VDATA", "T_VFLAG", "T_ELEM_OBS"),
		NewTable("T_UTLANDDATA", "T_UTLANDFLAG", "T_ELEM_OBS"),

		NewTable("T_10MINUTE_DATA", "T_10MINUTE_FLAG", "T_ELEM_OBS"),
		NewTable("T_ADATA_LEVEL", "T_AFLAG_LEVEL", "T_ELEM_OBS"),
		NewTable("T_MINUTE_DATA", "T_MINUTE_FLAG", "T_ELEM_OBS"),
		NewTable("T_SECOND_DATA", "T_SECOND_FLAG", "T_ELEM_OBS"),
		NewTable("T_CDCV_DATA", "T_CDCV_FLAG", "T_ELEM_EDATA"),
		NewTable("T_MERMAID", "T_MERMAID_FLAG", "T_ELEM_EDATA"),
		NewTable("T_SVVDATA", "T_SVVFLAG", "T_ELEM_OBS"),
		NewTable("T_AVINOR", "T_AVINOR_FLAG", "T_ELEM_OBS"),

		NewTable("T_MONTH", "T_MONTH_FLAG", "T_ELEM_MONTH"),
		NewTable("T_DIURNAL", "T_DIURNAL_FLAG", "T_ELEM_DIURNAL"),
		NewTable("T_HOMOGEN_DIURNAL", "", "T_ELEM_HOMOGEN_MONTH"),
		NewTable("T_HOMOGEN_MONTH", "", "T_ELEM_HOMOGEN_MONTH"),

		// Section 5: tables missing in the KDVH proxy:
		// 1. this one exists in a separate database
		// "T_PROJDATA": NewTable("T_PROJDATA", "T_PROJFLAG", "T_ELEM_PROJ", dumpDataAndFlags),
		// 2. these are not in active use and don't need to be imported in LARD
		// TODO: but should they be dumped?
		// "T_DIURNAL_INTERPOLATED": NewTable("T_DIURNAL_INTERPOLATED", "", "T_ELEM_DIURNAL", dumpDataAndFlags),
		// "T_MONTH_INTERPOLATED":   NewTable("T_MONTH_INTERPOLATED", "", "T_ELEM_MONTH", dumpDataAndFlags),
	}
}
