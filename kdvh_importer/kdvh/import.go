package kdvh

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickb777/period"

	"kdvh_importer/migrate"
	"kdvh_importer/utils"
)

// Used for lookup of fromtime and totime from KDVH
type KDVHKey struct {
	ParamKey
	Station int64
}

// ParamKey is used for lookup of parameter offsets and metadata from Stinfosys
type ParamKey struct {
	ElemCode  string `json:"ElemCode"`
	TableName string `json:"TableName"`
}

// Query from Stinfosys elem_map_cfnames_param
type Metadata struct {
	ElemCode  string
	TableName string
	TypeID    int32
	ParamID   int32
	Hlevel    int32
	Sensor    int32
	Fromtime  *time.Time
	// totime    *time.Time
}

// Metadata in KDVH tables
type MetaKDVH struct {
	TableName string
	Station   int64
	ElemCode  string
	FromTime  *time.Time
	ToTime    *time.Time
}

// Struct holding (almost) all the info needed from KDVH
type ObsKDVH struct {
	ID       int32
	Offset   period.Period
	ElemCode string
	ObsTime  time.Time
	Data     string
	Flags    string
}

type TimeseriesInfo struct {
	ID       int32
	Offset   period.Period
	ElemCode string
	Meta     *MetaKDVH
}

// TODO: need to extract scalar field
// Save metadata for later use by quering Stinfosys
func cacheStinfo(tables, elements []string) map[ParamKey]Metadata {
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

	for _, table := range KDVH_TABLES {
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

func cacheKDVH(tables, stations, elements []string) map[KDVHKey]*MetaKDVH {
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

	for _, t := range KDVH_TABLES {
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

func importTable(pool *pgxpool.Pool, table *KDVHTable, config *migrate.Config) {
	defer utils.SendEmailOnPanic("importTable", config.Email)

	if table.ImportUntil == 0 {
		// log.Printf("Skipping import of %s because this table is not set for import", table.TableName)
		return
	}

	slog.Info(fmt.Sprint("Starting import of ", table.TableName))
	utils.SetLogFile(table.TableName, "import")

	path := filepath.Join(config.BaseDir, table.TableName+"_combined")
	stations, err := os.ReadDir(path)
	if err != nil {
		slog.Warn(fmt.Sprintf("Could not read directory %s: %s", path, err))
		return
	}

	for _, station := range stations {
		stnr, err := getStationNumber(station, config.Stations)
		if err != nil {
			slog.Warn(err.Error())
			continue
		}

		stationDir := filepath.Join(path, station.Name())
		elements, err := os.ReadDir(stationDir)
		if err != nil {
			slog.Warn(fmt.Sprintf("Could not read directory %s: %s", stationDir, err))
			continue
		}

		var wg sync.WaitGroup
		for _, element := range elements {
			elemCode, err := getElementCode(element, config.Elements)
			if err != nil {
				slog.Warn(err.Error())
				continue
			}
			filename := filepath.Join(stationDir, element.Name())

			wg.Add(1)
			go func() {
				defer wg.Done()

				handle, err := os.Open(filename)
				if err != nil {
					slog.Warn(fmt.Sprintf("Could not open file '%s': %s", filename, err))
					return
				}
				defer handle.Close()

				timeseries, err := getTimeseries(elemCode, table.TableName, stnr, pool, config)
				if err != nil {
					slog.Error(fmt.Sprintf("%v - %v - %v: could not obtain timeseries, %s", table.TableName, stnr, elemCode, err))
					return
				}

				data, err := parseData(handle, timeseries, table, config)
				if err != nil {
					slog.Error(fmt.Sprintf("Could not parse file '%s': %s", filename, err))
					return
				}

				if len(data) == 0 {
					slog.Info(fmt.Sprintf("%v - %v - %v: no rows to insert (all obstimes > max import time)", table.TableName, stnr, elemCode))
					return
				}

				// TODO: we should be careful here, data shouldn't contain non-scalar params
				// Otherwise we need to insert to non-scalar table
				if !config.SkipData {
					count, err := insertData(pool, data)
					if err != nil {
						slog.Error(fmt.Sprintf("%v - %v - %v: failed data bulk insertion, %s", table.TableName, stnr, elemCode, err))
						return
					}

					logStr := fmt.Sprintf("%v - %v - %v: %v/%v data rows inserted", table.TableName, stnr, elemCode, count, len(data))
					if int(count) != len(data) {
						slog.Warn(logStr)
					} else {
						slog.Info(logStr)
					}
				}

				if !config.SkipFlags {
					count, err := insertFlags(pool, data)
					if err != nil {
						slog.Error(fmt.Sprintf("%v - %v - %v: failed flags bulk insertion, %s", table.TableName, stnr, elemCode, err))
						return
					}
					logStr := fmt.Sprintf("%v - %v - %v: %v/%v flags rows inserted", table.TableName, stnr, elemCode, count, len(data))
					if int(count) != len(data) {
						slog.Warn(logStr)
					} else {
						slog.Info(logStr)
					}
				}
			}()
		}
		wg.Wait()
	}

	log.SetOutput(os.Stdout)
	slog.Info(fmt.Sprint("Finished import of", table.TableName))
}

func getStationNumber(station os.DirEntry, stationList []string) (int64, error) {
	if !station.IsDir() {
		return 0, errors.New(fmt.Sprintf("%s is not a directory, skipping", station.Name()))
	}

	if stationList != nil && !slices.Contains(stationList, station.Name()) {
		return 0, errors.New(fmt.Sprintf("Station %v not in the list, skipping", station.Name()))
	}

	stnr, err := strconv.ParseInt(station.Name(), 10, 64)
	if err != nil {
		return 0, errors.New("Error parsing station number:" + err.Error())
	}

	return stnr, nil
}

func getElementCode(element os.DirEntry, elementList []string) (string, error) {
	elemCode := strings.ToUpper(strings.TrimSuffix(element.Name(), ".csv"))

	if elementList != nil && !slices.Contains(elementList, elemCode) {
		return "", errors.New(fmt.Sprintf("Element '%s' not in the list, skipping", elemCode))
	}

	if elemcodeIsInvalid(elemCode) {
		return "", errors.New(fmt.Sprintf("Element '%s' not set for import, skipping", elemCode))
	}
	return elemCode, nil
}

func parseData(handle io.Reader, ts *TimeseriesInfo, table *KDVHTable, config *migrate.Config) ([]migrate.ObsLARD, error) {
	scanner := bufio.NewScanner(handle)

	// Skip header if present
	if config.HasHeader {
		scanner.Scan()
	}

	var data []migrate.ObsLARD
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), config.Sep)

		obsTime, err := time.Parse("2006-01-02_15:04:05", cols[0])
		if err != nil {
			return nil, err
		}

		// only import data between kdvh's defined fromtime and totime
		// TODO: not 100% sure why we need this?
		if ts.Meta != nil {
			if ts.Meta.FromTime != nil && obsTime.Sub(*ts.Meta.FromTime) < 0 {
				continue
			}

			if ts.Meta.ToTime != nil && obsTime.Sub(*ts.Meta.ToTime) > 0 {
				break
			}
		}

		if obsTime.Year() >= table.ImportUntil {
			break
		}

		temp, err := table.ConvFunc(
			ObsKDVH{
				ID:       ts.ID,
				Offset:   ts.Offset,
				ElemCode: ts.ElemCode,
				ObsTime:  obsTime,
				Data:     cols[1],
				Flags:    cols[2],
			},
		)
		if err != nil {
			return nil, err
		}
		data = append(data, temp)
	}
	return data, nil
}

// TODO: benchmark double copyfrom vs batch insert?
func insertData(pool *pgxpool.Pool, data []migrate.ObsLARD) (int64, error) {
	return pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromSlice(len(data), func(i int) ([]any, error) {
			return []any{
				data[i].ID,
				data[i].ObsTime,
				// TODO: move to Data
				data[i].CorrKDVH,
			}, nil
		}),
	)
}

// TODO: need to insert to non-scalar table too

func insertFlags(pool *pgxpool.Pool, data []migrate.ObsLARD) (int64, error) {
	return pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"flags", "kdvh"},
		[]string{"timeseries", "obstime", "controlinfo", "useinfo"},
		pgx.CopyFromSlice(len(data), func(i int) ([]any, error) {
			return []any{
				data[i].ID,
				data[i].ObsTime,
				data[i].KVFlagControlInfo,
				data[i].KVFlagUseInfo,
			}, nil
		}),
	)
}

func getTimeseries(elemCode, tableName string, stnr int64, pool *pgxpool.Pool, config *migrate.Config) (*TimeseriesInfo, error) {
	var tsid int32
	key := ParamKey{elemCode, tableName}

	offset := config.OffsetMap[key]
	stinfoMeta, ok := config.StinfoMap[key]
	if !ok {
		// TODO: should it fail here? How do we deal with data without metadata?
		return nil, errors.New("Missing metadata in Stinfosys")
	}

	// if there's no metadata in KDVH we insert anyway
	kdvhMeta := config.KDVHMap[KDVHKey{key, stnr}]

	// Query LARD labels table with stinfosys metadata
	err := pool.QueryRow(
		context.TODO(),
		`SELECT timeseries FROM labels.met
            WHERE station_id = $1
            AND param_id = $2
            AND type_id = $3
            AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
            AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))`,
		stnr, stinfoMeta.ParamID, stinfoMeta.TypeID, stinfoMeta.Hlevel, stinfoMeta.Sensor).Scan(&tsid)

	// If timeseries exists, return its ID, offset and metadata
	if err == nil {
		return &TimeseriesInfo{tsid, offset, stinfoMeta.ElemCode, kdvhMeta}, nil
	}

	// Otherwise insert new timeseries
	transaction, err := pool.Begin(context.TODO())
	if err != nil {
		return nil, err
	}

	err = transaction.QueryRow(
		context.TODO(),
		`INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id`,
		stinfoMeta.Fromtime,
	).Scan(&tsid)
	if err != nil {
		return nil, err
	}

	_, err = transaction.Exec(
		context.TODO(),
		`INSERT INTO labels.met (timeseries, station_id, param_id, type_id, lvl, sensor)
            VALUES ($1, $2, $3, $4, $5, $6)`,
		tsid, stnr, stinfoMeta.ParamID, stinfoMeta.TypeID, stinfoMeta.Hlevel, stinfoMeta.Sensor)
	if err != nil {
		return nil, err
	}

	if err := transaction.Commit(context.TODO()); err != nil {
		return nil, err
	}

	return &TimeseriesInfo{tsid, offset, stinfoMeta.ElemCode, kdvhMeta}, nil
}

// TODO: add CALL_SIGN? It's not in stinfosys?
var INVALID_ELEMENTS = []string{"TYPEID", "TAM_NORMAL_9120", "RRA_NORMAL_9120", "OT", "OTN", "OTX", "DD06", "DD12", "DD18"}

func elemcodeIsInvalid(element string) bool {
	return strings.Contains(element, "KOPI") || slices.Contains(INVALID_ELEMENTS, element)
}
