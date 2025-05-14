package port

import (
	"fmt"
	"os"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickb777/period"

	kdvh "migrate/kdvh/db"
	"migrate/lard"
	"migrate/stinfosys"
)

type Cache struct {
	Offsets  OffsetMap
	Elements stinfosys.ElemMap
	Permits  stinfosys.PermitMaps
	Levels   stinfosys.ParamLevelMap
}

// Used for lookup of fromtime and totime from KDVH
type KDVHKey struct {
	Inner   stinfosys.Key
	Station int32
}

// Map of offsets used to correct KDVH times for specific parameters
type OffsetMap = map[stinfosys.Key]period.Period

// Caches all the metadata needed for import of KDVH tables.
// If any error occurs inside here the program will exit.
func CacheMetadata(tables, stations, elements []string, database []*Table) *Cache {
	stconn, ctx := stinfosys.Connect()
	defer stconn.Close(ctx)

	return &Cache{
		Elements: stinfosys.CacheElemMap(stconn),
		Permits:  stinfosys.NewPermitTables(stconn),
		Levels:   stinfosys.CacheParamLevels(stconn),
		Offsets:  cacheParamOffsets(),
	}
}

// TODO: these combinations are missing from `elem_map_cfnames_param`
// We could define a separate function for these special cases
// T_DIURNAL: {'FLRR', 'EV', 'AUDIT_DATE'}
// T_MONTH: {'AUDIT_DATE', 'RRA', 'TAMA', 'RR_NORMAL_9120'}
// T_METARDATA: {'X2R', 'VVD_METAR', 'X1R', 'ICAO_ID'}
// T_UTLANDDATA: {'TJINDX', 'QSI_24', 'RR_K816I'}
// T_ADATA: {'X2TAX_12', 'TDIF', 'QO', 'XX3', 'RR_010', 'ORIG_OBSTIME', 'RT_010', 'XX2', 'X2TAN_12', 'X2UUM_24', 'UUM_24'}
// T_TJ_DATA: {'X1TJ1'}
// T_MDATA: {'CALL_SIGN'}
// T_PDATA: {'CORRECTED'}
// T_VDATA: {'WA2', 'HW1', 'PW', 'RR_K816I', 'WA1', 'HW'}
//
// Available in `t_elem_map_cfnames`
// T_DIURNAL: {'FLRR'}
// T_MONTH: {}
// T_METARDATA: {}
// T_UTLANDDATA: {}
// T_ADATA: {'TDIF'}
// T_TJ_DATA: {}
// T_MDATA: {'CALL_SIGN'}
// T_PDATA: {}
// T_VDATA: {}

func GetTsInfoAndDbPool(table, element string, station int32, cache *Cache, pools *lard.Pools) (*kdvh.TsInfo, *pgxpool.Pool, error) {
	key := newKDVHKey(element, table, station)

	param, ok := cache.Elements[key.Inner]
	if !ok {
		return nil, nil, fmt.Errorf("missing metadata")
	}

	innerPool := pools.Restricted

	// Check if data for this station/element is restricted
	permit := cache.Permits.GetPermit(station, param.TypeID, param.ParamID)
	if permit != nil && *permit == 1 {
		innerPool = pools.Open
	}

	// No need to check for `!ok`, will default to 0 offset
	offset := cache.Offsets[key.Inner]

	label := lard.Label{
		StationID: station,
		TypeID:    param.TypeID,
		ParamID:   param.ParamID,
		Sensor:    &param.Sensor,
		LegacyLvl: param.Hlevel,
		Level:     cache.Levels.GetLevel(param.ParamID, param.Hlevel),
	}

	tsid, err := label.CreateKDVHTimeseries(element, table, &param.Fromtime, permit, innerPool)
	if err != nil {
		return nil, nil, err
	}

	return &kdvh.TsInfo{
		Id:       tsid,
		Station:  station,
		Element:  element,
		Offset:   offset,
		IsScalar: param.IsScalar,
	}, innerPool, nil
}

func newKDVHKey(elem, table string, stnr int32) KDVHKey {
	return KDVHKey{stinfosys.Key{ElemCode: elem, TableName: table}, stnr}
}

// Caches how to modify the obstime (in KDVH) for certain paramids
func cacheParamOffsets() OffsetMap {
	fmt.Printf("%-50s", "Caching product_offsets.csv... ")
	cache := make(OffsetMap)

	type CSVRow struct {
		TableName      string `csv:"table_name"`
		ElemCode       string `csv:"elem_code"`
		ParamID        int32  `csv:"paramid"`
		FromtimeOffset string `csv:"fromtime_offset"`
		Timespan       string `csv:"timespan"`
	}

	csvfile, err := os.Open("kdvh/product_offsets.csv")
	if err != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}
	defer csvfile.Close()

	var csvrows []CSVRow
	if err := gocsv.UnmarshalFile(csvfile, &csvrows); err != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}

	for _, row := range csvrows {
		var fromtimeOffset, timespan period.Period
		if row.FromtimeOffset != "" {
			fromtimeOffset, err = period.Parse(row.FromtimeOffset)
			if err != nil {
				fmt.Println("\n", err)
				os.Exit(1)
			}
		}
		if row.Timespan != "" {
			timespan, err = period.Parse(row.Timespan)
			if err != nil {
				fmt.Println("\n", err)
				os.Exit(1)
			}
		}
		migrationOffset, err := fromtimeOffset.Add(timespan)
		if err != nil {
			fmt.Println("\n", err)
			os.Exit(1)
		}

		cache[stinfosys.Key{ElemCode: row.ElemCode, TableName: row.TableName}] = migrationOffset
	}

	fmt.Println("Done!")
	return cache
}
