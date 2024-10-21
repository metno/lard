package kdvh

import (
	"kdvh_importer/lard"
	"time"

	"github.com/rickb777/period"
)

// In KDVH for each table name we usually have three separate tables:
// 1. A T_{} table containing the observation values;
// 2. A T_{}_FLAG table containing the quality control (QC) flags;
// 3. A T_ELEM_{} table containing metadata regarding the validity of timeseries.
//
// Data and flag tables have the same schema:
// | dato | stnr | ... |
// where 'dato' is the timestamp of the observation, 'stnr' is the station
// where the observation was measured, and '...' is a varying number of columns
// each with a different observation where the column name is the 'elem_code'
// (e.g. for air temperature, 'ta').
//
// The T_ELEM tables have the following schema:
// | stnr | elem_code | fdato | tdato | table_name | flag_table_name | audit_dato

// KDVHTable contain metadata on how to treat different tables in KDVH
type KDVHTable struct {
	TableName     string          // Name of the table with observations
	FlagTableName string          // Name of the table with QC flags
	ElemTableName string          // Name of the table with metadata
	DumpFunc      DumpFunction    // Function used to dump the KDVH table
	ConvFunc      ConvertFunction // Function that converts KDVH obs to LARD obs
	ImportUntil   int             // Migrate data only until the year specified by this field
}

func newKDVHTable(data, flag, elem string) *KDVHTable {
	return &KDVHTable{
		TableName:     data,
		FlagTableName: flag,
		ElemTableName: elem,
		DumpFunc:      dumpDataAndFlags,
		ConvFunc:      makeDataPage,
	}
}

// Sets the latest import year
func (t *KDVHTable) SetImport(year int) *KDVHTable {
	t.ImportUntil = year
	return t
}

// Sets the function used to dump the KDVH table
func (t *KDVHTable) SetDumpFunc(f DumpFunction) *KDVHTable {
	t.DumpFunc = f
	return t
}

// Sets the function used to convert observations from the table to LARD observations
func (t *KDVHTable) SetConvFunc(f ConvertFunction) *KDVHTable {
	t.ConvFunc = f
	return t
}

// The KDVH database simply contains a vector of KDVHTables
// TODO: should we have different types for each table???
type KDVH struct {
	// Tables []dump.Dumper
	Tables []*KDVHTable
}

func Init() *KDVH {
	return &KDVH{
		// []dump.Dumper{
		[]*KDVHTable{
			// Section 1: tables that need to be migrated entirely
			// TODO: figure out if we need to use the elem_code_paramid_level_sensor_t_edata table?
			newKDVHTable("T_EDATA", "T_EFLAG", "T_ELEM_EDATA").SetConvFunc(makeDataPageEdata).SetImport(3000),
			// NOTE(1): there is a T_METARFLAG, but it's empty
			// NOTE(2): already dumped, but with wrong format?
			newKDVHTable("T_METARDATA", "", "T_ELEM_METARDATA").SetDumpFunc(dumpDataOnly).SetImport(3000), // already dumped

			// Section 2: tables with some data in kvalobs, import only up to 2005-12-31
			newKDVHTable("T_ADATA", "T_AFLAG", "T_ELEM_OBS").SetImport(2006),
			newKDVHTable("T_MDATA", "T_MFLAG", "T_ELEM_OBS").SetImport(2006),                                // already dumped
			newKDVHTable("T_TJ_DATA", "T_TJ_FLAG", "T_ELEM_OBS").SetImport(2006),                            // already dumped
			newKDVHTable("T_PDATA", "T_PFLAG", "T_ELEM_OBS").SetConvFunc(makeDataPagePdata).SetImport(2006), // already dumped
			newKDVHTable("T_NDATA", "T_NFLAG", "T_ELEM_OBS").SetConvFunc(makeDataPageNdata).SetImport(2006), // already dumped
			newKDVHTable("T_VDATA", "T_VFLAG", "T_ELEM_OBS").SetConvFunc(makeDataPageVdata).SetImport(2006), // already dumped
			newKDVHTable("T_UTLANDDATA", "T_UTLANDFLAG", "T_ELEM_OBS").SetImport(2006),                      // already dumped

			// Section 3: tables that should only be dumped
			newKDVHTable("T_10MINUTE_DATA", "T_10MINUTE_FLAG", "T_ELEM_OBS").SetDumpFunc(dumpByYear),
			newKDVHTable("T_ADATA_LEVEL", "T_AFLAG_LEVEL", "T_ELEM_OBS"),
			newKDVHTable("T_AVINOR", "T_AVINOR_FLAG", "T_ELEM_OBS"),
			// FIXME: T_PROJFLAG is not in the proxy!
			newKDVHTable("T_PROJDATA", "T_PROJFLAG", "T_ELEM_PROJ"),
			newKDVHTable("T_MINUTE_DATA", "T_MINUTE_FLAG", "T_ELEM_OBS").SetDumpFunc(dumpByYear), // already dumped
			newKDVHTable("T_SECOND_DATA", "T_SECOND_FLAG", "T_ELEM_OBS").SetDumpFunc(dumpByYear), // already dumped
			newKDVHTable("T_CDCV_DATA", "T_CDCV_FLAG", "T_ELEM_EDATA"),                           // already dumped
			newKDVHTable("T_MERMAID", "T_MERMAID_FLAG", "T_ELEM_EDATA"),                          // already dumped
			newKDVHTable("T_SVVDATA", "T_SVVFLAG", "T_ELEM_OBS"),                                 // already dumped

			// Section 4: other special cases
			newKDVHTable("T_10MINUTE_DATA", "T_10MINUTE_FLAG", "").SetDumpFunc(dumpByYear),
			newKDVHTable("T_MINUTE_DATA", "T_MINUTE_FLAG", "").SetDumpFunc(dumpByYear),
			newKDVHTable("T_SECOND_DATA", "T_SECOND_FLAG", "").SetDumpFunc(dumpByYear),

			// TODO: don't know why these have a special convertion function if they are not to be imported
			newKDVHTable("T_MONTH", "T_MONTH_FLAG", "T_ELEM_MONTH").SetConvFunc(makeDataPageProduct).SetImport(1957),
			newKDVHTable("T_DIURNAL", "T_DIURNAL_FLAG", "T_ELEM_DIURNAL").SetConvFunc(makeDataPageProduct),
			newKDVHTable("T_HOMOGEN_DIURNAL", "", "T_ELEM_HOMOGEN_MONTH").SetDumpFunc(dumpDataOnly).SetConvFunc(makeDataPageProduct),
			newKDVHTable("T_HOMOGEN_MONTH", "T_ELEM_HOMOGEN_MONTH", "").SetDumpFunc(dumpHomogenMonth).SetConvFunc(makeDataPageProduct),

			// TODO: these two are the only tables seemingly missing from the KDVH proxy
			// {TableName: "T_DIURNAL_INTERPOLATED", DataFunction: makeDataPageDiurnalInterpolated, ImportUntil: 3000},
			// {TableName: "T_MONTH_INTERPOLATED", DataFunction: makeDataPageDiurnalInterpolated, ImportUntil: 3000},
		},
	}
}

// Mimics dump.Config
type DumpConfig struct {
	Tables    []string
	Stations  []string
	Elements  []string
	Email     []string
	BaseDir   string
	Overwrite bool
}

// Mimics migrate.Config
type MigrateConfig struct {
	Tables    []string
	Stations  []string
	Elements  []string
	Email     []string
	BaseDir   string
	Verbose   bool
	SkipData  bool
	SkipFlags bool
	HasHeader bool
	Sep       string
	OffsetMap map[ParamKey]period.Period // Map of offsets used to correct (?) KDVH times for specific parameters
	StinfoMap map[ParamKey]Metadata      // Map of metadata used to query timeseries ID in LARD
	KDVHMap   map[KDVHKey]*MetaKDVH      // Map of from_time and to_time for each (table, station, element) triplet
}

// 'ConvertFunction's convert from KDVH to LARD observations
type ConvertFunction func(Obs) (ObsLard, error)

// This type contains all data needed to be inserted in LARD
// We split it into 'kdvh_importer/lard' types inside parseData
type ObsLard struct {
	// Unique timeseries identifier
	ID int32
	// Time of observation
	ObsTime time.Time
	// Observation data formatted as a double precision floating point
	Data *float64
	// Observation data that cannot be represented as a float
	NonScalarData *string
	// Flag encoding quality control status
	KVFlagControlInfo string
	// Flag encoding quality control status
	KVFlagUseInfo string
	// Subset of 5 digits of KVFlagUseInfo stored in KDVH
	// KDVHFlag []byte
	// Comma separated value listing checks that failed during quality control
	// KVCheckFailed string
}

// TODO: we don't even need to this separate it into three different structs?
// Just loop over the same vector thrice?
func (o *ObsLard) toObs() lard.Obs {
	return lard.Obs{
		ID:      o.ID,
		ObsTime: o.ObsTime,
		Data:    *o.Data,
	}
}

func (o *ObsLard) toNonscalar() lard.NonScalarObs {
	return lard.NonScalarObs{
		ID:      o.ID,
		ObsTime: o.ObsTime,
		Data:    o.NonScalarData,
	}
}

func (o *ObsLard) toFlags() lard.Flags {
	return lard.Flags{
		ID:                o.ID,
		ObsTime:           o.ObsTime,
		KVFlagUseInfo:     o.KVFlagUseInfo,
		KVFlagControlInfo: o.KVFlagControlInfo,
	}
}
