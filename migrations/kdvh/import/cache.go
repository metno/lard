package port

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickb777/period"

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

func (cache *Cache) NewTsInfo(table, element string, station int32, pool *pgxpool.Pool) (*kdvh.TsInfo, error) {
	logstr := fmt.Sprintf("[%v - %v - %v]: ", table, station, element)
	key := newKDVHKey(element, table, station)

	param, ok := cache.Elements[key.Inner]
	if !ok {
		// TODO: should it fail here? How do we deal with data without metadata?
		slog.Error(logstr + "Missing metadata in Stinfosys")
		return nil, errors.New("No metadata")
	}

	// Check if data for this station/element is restricted
	// TODO: eventually use this to choose which table to use on insert
	isOpen := cache.Permits.TimeseriesIsOpen(station, param.TypeID, param.ParamID)
	if !isOpen {
		slog.Warn(logstr + "Timeseries data is restricted")
		return nil, errors.New("Restricted data")
	}

	// No need to check for `!ok`, will default to 0 offset
	offset := cache.Offsets[key.Inner]

	// No need to check for `!ok`, timespan will be ignored if not in the map
	timespan, ok := cache.Timespans[key]

	label := lard.Label{
		StationID: station,
		TypeID:    param.TypeID,
		ParamID:   param.ParamID,
		Sensor:    &param.Sensor,
		Level:     param.Hlevel,
	}

	// TODO: are Param.Fromtime and Span.From different?
	if timespan.From != nil {
		slog.Info(fmt.Sprintf("stinfo.fromtime %v - kdvh.fromtime - %v", param.Fromtime, timespan.From))
	}

	tsid, err := lard.GetTimeseriesID(&label, utils.TimeSpan{From: &param.Fromtime, To: timespan.To}, pool)
	if err != nil {
		slog.Error(logstr + "could not obtain timeseries - " + err.Error())
		return nil, err
	}

	return &kdvh.TsInfo{
		Id:       tsid,
		Station:  station,
		Element:  element,
		Offset:   offset,
		Param:    param,
		Timespan: timespan,
		Logstr:   logstr,
	}, nil
}

func newKDVHKey(elem, table string, stnr int32) KDVHKey {
	return KDVHKey{stinfosys.Key{ElemCode: elem, TableName: table}, stnr}
}

// Cache timeseries timespan from KDVH
func cacheKDVH(tables, stations, elements []string, database []*Table) KDVHMap {
	cache := make(KDVHMap)

	slog.Info("Connecting to KDVH proxy to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv(kdvh.KDVH_ENV_VAR))
	if err != nil {
		slog.Error("Could not connect to KDVH proxy. Make sure to be connected to the VPN: " + err.Error())
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
			slog.Error(err.Error())
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
				slog.Error(err.Error())
				os.Exit(1)
			}

			cache[key] = span
		}

		if rows.Err() != nil {
			slog.Error(rows.Err().Error())
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
		slog.Error(err.Error())
		os.Exit(1)
	}
	defer csvfile.Close()

	var csvrows []CSVRow
	if err := gocsv.UnmarshalFile(csvfile, &csvrows); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	for _, row := range csvrows {
		var fromtimeOffset, timespan period.Period
		if row.FromtimeOffset != "" {
			fromtimeOffset, err = period.Parse(row.FromtimeOffset)
			if err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}
		}
		if row.Timespan != "" {
			timespan, err = period.Parse(row.Timespan)
			if err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}
		}
		migrationOffset, err := fromtimeOffset.Add(timespan)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		cache[stinfosys.Key{ElemCode: row.ElemCode, TableName: row.TableName}] = migrationOffset
	}

	return cache
}
