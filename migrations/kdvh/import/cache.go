package port

import (
	"context"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickb777/period"
	"github.com/rs/zerolog/log"

	kdvh "migrate/kdvh/db"
	"migrate/lard"
	"migrate/stinfosys"
	"migrate/utils"
)

type Cache struct {
	Offsets   OffsetMap
	Timespans KDVHMap
	Elements  stinfosys.ElemMap
	Permits   stinfosys.PermitMaps
}

// Map of `from_time` and `to_time` for each (table, station, element) triplet. Not present for all parameters
type KDVHMap = map[KDVHKey]utils.TimeSpan

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
		Elements:  stinfosys.CacheElemMap(stconn),
		Permits:   stinfosys.NewPermitTables(stconn),
		Offsets:   cacheParamOffsets(),
		Timespans: cacheKDVH(tables, stations, elements, database),
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

func (cache *Cache) NewTsInfo(table, element string, station int32, pool *pgxpool.Pool) (*kdvh.TsInfo, error) {
	key := newKDVHKey(element, table, station)

	param, ok := cache.Elements[key.Inner]
	if !ok {
		log.Error().
			Str("table_name", table).
			Int32("station", station).
			Str("element", element).
			Msg("Missing metadata in Stinfosys `elem_map_cfnames_param` table")
		// TODO: have a local map that contains whether the params are scalar or not and if they have
		// a sibling paramid
		return nil, fmt.Errorf("No metadata")
	}

	// Check if data for this station/element is restricted
	// TODO: eventually use this to choose which table to use on insert
	isOpen := cache.Permits.TimeseriesIsOpen(station, param.TypeID, param.ParamID)
	if !isOpen {
		log.Warn().
			Str("table_name", table).
			Int32("station", station).
			Str("element", element).
			Msg("Timeseries data is restricted")
		return nil, fmt.Errorf("Restricted data")
	}

	// No need to check for `!ok`, will default to 0 offset
	offset := cache.Offsets[key.Inner]

	// Get timespan found in KDVH
	// No need to check for `!ok`, timespan will be ignored if not in the map
	timespan, ok := cache.Timespans[key]

	label := lard.Label{
		StationID: station,
		TypeID:    param.TypeID,
		ParamID:   param.ParamID,
		Sensor:    &param.Sensor,
		Level:     param.Hlevel,
	}

	if timespan.From != nil {
		log.Info().Time("stinfo.fromtime", param.Fromtime).Time("kdvh.fromtime", *timespan.From).Msg("")
	}

	tsSpan := utils.TimeSpan{From: &param.Fromtime, To: timespan.To}
	tsid, err := label.CreateKDVHTimeseries(element, table, tsSpan, pool)
	if err != nil {
		log.Error().Err(err).
			Str("table_name", table).
			Int32("station", station).
			Str("element", element).
			Msg("")
		return nil, err
	}

	return &kdvh.TsInfo{
		Id:       tsid,
		Station:  station,
		Element:  element,
		Offset:   offset,
		IsScalar: param.IsScalar,
		Timespan: timespan,
	}, nil
}

func newKDVHKey(elem, table string, stnr int32) KDVHKey {
	return KDVHKey{stinfosys.Key{ElemCode: elem, TableName: table}, stnr}
}

// Cache timeseries timespan from KDVH
// TODO: we should dump these tables! We will not be able to connect to KDVH when it's taken down
func cacheKDVH(tables, stations, elements []string, database []*Table) KDVHMap {
	cache := make(KDVHMap)

	log.Info().Msg("Connecting to KDVH proxy to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv(kdvh.KDVH_ENV_VAR))
	if err != nil {
		log.Error().Err(err).Msg("Could not connect to KDVH proxy. Make sure to be connected to the VPN")
		os.Exit(1)
	}
	defer conn.Close(context.TODO())

	for _, t := range database {
		if len(tables) > 0 && !slices.Contains(tables, t.TableName) {
			continue
		}

		// TODO: probably need to sanitize these inputs
		query := fmt.Sprintf(
			`SELECT table_name, stnr, elem_code, fdato, tdato FROM %s
                WHERE ($1::bigint[] = '{}' OR stnr = ANY($1))
                AND ($2::text[] = '{}' OR elem_code = ANY($2))`,
			t.ElemTableName,
		)

		rows, err := conn.Query(context.TODO(), query, stations, elements)
		if err != nil {
			log.Error().Err(err).Msg("")
			os.Exit(1)
		}

		for rows.Next() {
			var key KDVHKey
			var span utils.TimeSpan

			err := rows.Scan(
				&key.Inner.TableName,
				&key.Station,
				&key.Inner.ElemCode,
				&span.From,
				&span.To,
			)

			if err != nil {
				log.Error().Err(err).Msg("")
				os.Exit(1)
			}

			cache[key] = span
		}

		if rows.Err() != nil {
			log.Error().Err(rows.Err()).Msg("")
			os.Exit(1)
		}

	}

	return cache
}

// Caches how to modify the obstime (in KDVH) for certain paramids
func cacheParamOffsets() OffsetMap {
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
		log.Error().Err(err).Msg("")
		os.Exit(1)
	}
	defer csvfile.Close()

	var csvrows []CSVRow
	if err := gocsv.UnmarshalFile(csvfile, &csvrows); err != nil {
		log.Error().Err(err).Msg("")
		os.Exit(1)
	}

	for _, row := range csvrows {
		var fromtimeOffset, timespan period.Period
		if row.FromtimeOffset != "" {
			fromtimeOffset, err = period.Parse(row.FromtimeOffset)
			if err != nil {
				log.Error().Err(err).Msg("")
				os.Exit(1)
			}
		}
		if row.Timespan != "" {
			timespan, err = period.Parse(row.Timespan)
			if err != nil {
				log.Error().Err(err).Msg("")
				os.Exit(1)
			}
		}
		migrationOffset, err := fromtimeOffset.Add(timespan)
		if err != nil {
			log.Error().Err(err).Msg("")
			os.Exit(1)
		}

		cache[stinfosys.Key{ElemCode: row.ElemCode, TableName: row.TableName}] = migrationOffset
	}

	return cache
}
