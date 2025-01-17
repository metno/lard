package dump

import "github.com/jackc/pgx/v5/pgxpool"

type DumpFunction func(path, element, station, dataTable, flagTable string, logStr string, pool *pgxpool.Pool) error
type Table struct {
	TableName     string       // Name of the DATA table
	FlagTableName string       // Name of the FLAG table
	ElemTableName string       // Name of the ELEM table
	dumpInner     DumpFunction // How to dump a given combo of (element, station) for the given table
}

func (table *Table) DumpFn(path, element, station, logStr string, pool *pgxpool.Pool) error {
	return table.dumpInner(path, element, station, table.TableName, table.FlagTableName, logStr, pool)
}

func NewTable(data, flag, elem string, fn DumpFunction) *Table {
	return &Table{
		TableName:     data,
		FlagTableName: flag,
		ElemTableName: elem,
		dumpInner:     fn,
	}
}

func InitDump() []*Table {
	return []*Table{
		// Section 1: tables that need to be migrated entirely
		NewTable("T_EDATA", "T_EFLAG", "T_ELEM_EDATA", dumpDataAndFlags),
		NewTable("T_METARDATA", "", "T_ELEM_FDATA", dumpDataOnly),

		NewTable("T_ADATA", "T_AFLAG", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_MDATA", "T_MFLAG", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_TJ_DATA", "T_TJ_FLAG", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_PDATA", "T_PFLAG", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_NDATA", "T_NFLAG", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_VDATA", "T_VFLAG", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_UTLANDDATA", "T_UTLANDFLAG", "T_ELEM_OBS", dumpDataAndFlags),

		NewTable("T_10MINUTE_DATA", "T_10MINUTE_FLAG", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_ADATA_LEVEL", "T_AFLAG_LEVEL", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_MINUTE_DATA", "T_MINUTE_FLAG", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_SECOND_DATA", "T_SECOND_FLAG", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_CDCV_DATA", "T_CDCV_FLAG", "T_ELEM_EDATA", dumpDataAndFlags),
		NewTable("T_MERMAID", "T_MERMAID_FLAG", "T_ELEM_EDATA", dumpDataAndFlags),
		NewTable("T_SVVDATA", "T_SVVFLAG", "T_ELEM_OBS", dumpDataAndFlags),
		NewTable("T_AVINOR", "T_AVINOR_FLAG", "T_ELEM_OBS", dumpDataAndFlags),

		NewTable("T_MONTH", "T_MONTH_FLAG", "T_ELEM_MONTH", dumpDataAndFlags),
		NewTable("T_DIURNAL", "T_DIURNAL_FLAG", "T_ELEM_DIURNAL", dumpDataAndFlags),
		NewTable("T_HOMOGEN_DIURNAL", "", "T_ELEM_HOMOGEN_MONTH", dumpDataOnly),
		NewTable("T_HOMOGEN_MONTH", "", "T_ELEM_HOMOGEN_MONTH", dumpHomogenMonth),

		// Section 5: tables missing in the KDVH proxy:
		// 1. this one exists in a separate database
		// "T_PROJDATA": NewTable("T_PROJDATA", "T_PROJFLAG", "T_ELEM_PROJ", dumpDataAndFlags),
		// 2. these are not in active use and don't need to be imported in LARD
		// "T_DIURNAL_INTERPOLATED": NewTable("T_DIURNAL_INTERPOLATED", "", "T_ELEM_DIURNAL", dumpDataAndFlags),
		// "T_MONTH_INTERPOLATED":   NewTable("T_MONTH_INTERPOLATED", "", "T_ELEM_MONTH", dumpDataAndFlags),
	}
}
