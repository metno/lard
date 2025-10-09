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
}

func NewTable(data, flag, elem string) *Table {
	var query string

	// Set the query string
	switch data {
	case "T_HOMOGEN_MONTH":
		// T_HOMOGEN_MONTH contains seasonal and annual data, plus other derivative
		// data combining both of these. We decided to dump only the monthly data (season BETWEEN 1 AND 12) for
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

	// TODO: maybe merge with T_METARDATA and a COALESCE
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
		query = `
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
		) f USING(dato)`
	}

	return &Table{
		TableName:     data,
		FlagTableName: flag,
		ElemTableName: elem,
		Query:         query,
	}
}

func InitDump() []*Table {
	return []*Table{
		// Section 1: tables that need to be migrated entirely
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
		// "T_DIURNAL_INTERPOLATED": NewTable("T_DIURNAL_INTERPOLATED", "", "T_ELEM_DIURNAL", dumpDataAndFlags),
		// "T_MONTH_INTERPOLATED":   NewTable("T_MONTH_INTERPOLATED", "", "T_ELEM_MONTH", dumpDataAndFlags),
	}
}
