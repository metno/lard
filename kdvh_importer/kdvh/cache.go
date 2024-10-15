package kdvh

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5"
	"github.com/rickb777/period"
)

func (config *ImportConfig) cacheMetadata(kdvh *KDVH) {
	config.OffsetMap = cacheParamOffsets()
	config.StinfoMap = kdvh.cacheStinfo(config.Tables, config.Elements)
	config.KDVHMap = kdvh.cacheKDVH(config.Tables, config.Stations, config.Elements)
}

// TODO: need to extract scalar field
// Save metadata for later use by quering Stinfosys
func (db *KDVH) cacheStinfo(tables, elements []string) map[ParamKey]Metadata {
	cache := make(map[ParamKey]Metadata)

	slog.Info("Connecting to Stinfosys to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("STINFO_STRING"))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Stinfosys. Make sure to be connected to the VPN.", err))
		os.Exit(1)
	}
	defer conn.Close(context.TODO())

	for _, table := range db.Tables {
		if tables != nil && !slices.Contains(tables, table.TableName) {
			continue
		}

		query := `SELECT elem_code, table_name, typeid, paramid, hlevel, sensor, fromtime
                    FROM elem_map_cfnames_param
                    WHERE table_name = $1
                    AND ($2::text[] IS NULL OR elem_code = ANY($2))`

		rows, err := conn.Query(context.TODO(), query, table.TableName, elements)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		// TODO: eventually move to RowToStructByName (less brittle, but requires adding tags to the struct)
		metas, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Metadata])
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		for _, meta := range metas {
			// log.Println(meta)
			cache[ParamKey{meta.ElemCode, meta.TableName}] = meta
		}
	}

	return cache
}

func (db *KDVH) cacheKDVH(tables, stations, elements []string) map[KDVHKey]*MetaKDVH {
	cache := make(map[KDVHKey]*MetaKDVH)

	slog.Info("Connecting to KDVH proxy to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("KDVH_PROXY_CONN"))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to KDVH proxy. Make sure to be connected to the VPN.", err))
		os.Exit(1)
	}
	defer conn.Close(context.TODO())

	for _, t := range db.Tables {
		if tables != nil && !slices.Contains(tables, t.TableName) {
			continue
		}

		// TODO: probably need to sanitize these inputs
		query := fmt.Sprintf(
			`SELECT table_name, stnr, elem_code, fdato, tdato FROM %s
                WHERE ($1::bigint[] IS NULL OR stnr = ANY($1))
                AND ($2::text[] IS NULL OR elem_code = ANY($2))`,
			t.ElemTableName,
		)

		rows, err := conn.Query(context.TODO(), query, stations, elements)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		metas, err := pgx.CollectRows(rows, pgx.RowToStructByPos[MetaKDVH])
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		for _, meta := range metas {
			cache[KDVHKey{ParamKey{meta.ElemCode, meta.TableName}, meta.Station}] = &meta
		}
	}

	return cache
}

// how to modify the obstime (in kdvh) for certain paramid
func cacheParamOffsets() map[ParamKey]period.Period {
	cache := make(map[ParamKey]period.Period)

	type CSVRow struct {
		TableName      string `csv:"table_name"`
		ElemCode       string `csv:"elem_code"`
		ParamID        int64  `csv:"paramid"`
		FromtimeOffset string `csv:"fromtime_offset"`
		Timespan       string `csv:"timespan"`
	}
	csvfile, err := os.Open("product_offsets.csv")
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

		cache[ParamKey{ElemCode: row.ElemCode, TableName: row.TableName}] = migrationOffset
	}

	return cache
}
