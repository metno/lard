package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
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
)

// ParamKey is used for lookup of parameter offsets and metadata
type ParamKey struct {
	ElemCode  string `json:"ElemCode"`
	TableName string `json:"TableName"`
}

// Query from elem_map_cfnames_param
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

// TODO: define the schema we want to export here
// And probably we should use nullable types?
type ObsLARD struct {
	// Unique timeseries identifier
	ID int32
	// Time of observation
	ObsTime time.Time
	// Observation data formatted as a double precision floating point
	Data float64
	// Observation data formatted as a binary large object.
	// TODO: what is this?
	DataBlob []byte
	// Estimated observation data from KDVH (unknown method)
	// NOTE: unknown method ??????????
	CorrKDVH float64
	// Latest updated corrected observation data value
	// Corrected float64
	// Flag encoding quality control status
	KVFlagControlInfo []byte
	// Flag encoding quality control status
	KVFlagUseInfo []byte
	// Subset of 5 digits of KVFlagUseInfo stored in KDVH
	// KDVHFlag []byte
	// Comma separated value listing checks that failed during quality control
	KVCheckFailed string
}

// Struct holding all the info needed from KDVH
type ObsKDVH struct {
	*Timeseries
	ObsTime time.Time
	Data    string
	Flags   string
}

type Timeseries struct {
	ID     int32
	Offset period.Period
	Metadata
}

// KDVHKey contains the important keys from the fetchkdvh labeltype
// type KDVHKey struct {
// 	Stnr int64 `json:"Stnr"`
// 	ParamKey
// }

// TODO: this would be queried directly from KDVH?
// better than querying stinfosys?
// type TimeSeriesInfoKDVH struct {
// 	Stnr     int64      `db:"stnr"`
// 	ElemCode string     `db:"elem_code"`
// 	FromTime *time.Time `db:"fdato"`
// 	ToTime   *time.Time `db:"tdato"`
// }

type ImportArgs struct {
	BaseDir     string   `long:"dir" required:"true" description:"Base directory where the dumped data is stored"`
	Sep         string   `long:"sep" default:";"  description:"Separator character in the dumped files"`
	TablesCmd   string   `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationsCmd string   `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElementsCmd string   `long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	Email       []string `long:"email" description:"Optional email address used to notify if the program crashed"`
	Tables      []string
	Stations    []string
	Elements    []string
	OffsetMap   map[ParamKey]period.Period // Map of offsets used to correct (?) KDVH times for specific parameters
	MetadataMap map[ParamKey]Metadata      // Map of metadata used to query timeseries id in LARD
}

// Updates ImportArgs:
// 1. Populates slices by parsing the string provided via cmd
// 2. Caches time offsets by reading 'param_offset.csv'
// 3. Caches metadata from Stinfosys
func (args *ImportArgs) updateConfig() {
	if args.TablesCmd != "" {
		args.Tables = strings.Split(args.TablesCmd, ",")
	}
	if args.StationsCmd != "" {
		args.Stations = strings.Split(args.StationsCmd, ",")
	}
	if args.ElementsCmd != "" {
		args.Elements = strings.Split(args.ElementsCmd, ",")
	}

	args.OffsetMap = cacheParamOffsets()
	args.MetadataMap = cacheMetadata(args.Tables, args.Elements)
}

// This method is automatically called by go-flags while parsing the cmd
func (args *ImportArgs) Execute(_ []string) error {
	args.updateConfig()

	// Create connection pool for LARD
	pool, err := pgxpool.New(context.TODO(), os.Getenv("LARD_STRING"))
	if err != nil {
		log.Fatalln("Could not connect to Lard:", err)
	}
	defer pool.Close()

	for name, table := range TABLE2INSTRUCTIONS {
		if args.Tables != nil && !slices.Contains(args.Tables, name) {
			continue
		}
		importTable(pool, table, args)
	}

	return nil
}

// Save metadata for later use by quering Stinfosys
// TODO: do this directly in the main loop?
func cacheMetadata(tables, elements []string) map[ParamKey]Metadata {
	cache := make(map[ParamKey]Metadata)

	log.Println("Connecting to Stinfosys to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("STINFO_STRING"))
	if err != nil {
		log.Fatalln("Could not connect to Stinfosys. Make sure to be connected to the VPN.", err)
	}
	defer conn.Close(context.TODO())

	for tableName := range TABLE2INSTRUCTIONS {
		if tables != nil && !slices.Contains(tables, tableName) {
			continue
		}

		query := `SELECT elem_code, table_name, typeid, paramid, hlevel, sensor, fromtime
                    FROM elem_map_cfnames_param
                    WHERE table_name = $1 
                    AND ($2::text[] IS NULL OR elem_code = ANY($2))`

		rows, err := conn.Query(context.TODO(), query, tableName, elements)
		if err != nil {
			log.Fatalln(err)
		}

		metas, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Metadata])
		if err != nil {
			log.Fatalln(err)
		}

		for _, meta := range metas {
			cache[ParamKey{meta.TableName, meta.ElemCode}] = meta
		}
	}

	return cache
}

// how to modify the obstime (in kdvh) for certain paramid
func cacheParamOffsets() map[ParamKey]period.Period {
	cache := make(map[ParamKey]period.Period)

	// TODO: which table does product_offsets.csv come from?
	type CSVRow struct {
		TableName      string `csv:"table_name"`
		ElemCode       string `csv:"elem_code"`
		ParamID        int64  `csv:"paramid"`
		FromtimeOffset string `csv:"fromtime_offset"`
		Timespan       string `csv:"timespan"`
	}
	csvfile, err := os.Open("product_offsets.csv")
	if err != nil {
		log.Fatalln(err)
	}
	defer csvfile.Close()

	var csvrows []CSVRow
	if err := gocsv.UnmarshalFile(csvfile, &csvrows); err != nil {
		log.Fatalln(err)
	}

	for _, row := range csvrows {
		var fromtimeOffset, timespan period.Period
		if row.FromtimeOffset != "" {
			fromtimeOffset, err = period.Parse(row.FromtimeOffset)
			if err != nil {
				log.Fatalln(err)
			}
		}
		if row.Timespan != "" {
			timespan, err = period.Parse(row.Timespan)
			if err != nil {
				log.Fatalln(err)
			}
		}
		migrationOffset, err := fromtimeOffset.Add(timespan)
		if err != nil {
			log.Fatalln(err)
		}

		cache[ParamKey{ElemCode: row.ElemCode, TableName: row.TableName}] = migrationOffset
	}

	return cache
}

func importTable(pool *pgxpool.Pool, table *TableInstructions, config *ImportArgs) {
	defer sendEmailOnPanic("importTable", config.Email)

	if table.ImportUntil == 0 {
		// log.Printf("Skipping import of %s because this table is not set for import\n", table.TableName)
		return
	}

	log.Println("Starting import of", table.TableName)
	setLogFile(table.TableName, "import")

	path := filepath.Join(config.BaseDir, table.TableName+"_combined")
	stations, err := os.ReadDir(path)
	if err != nil {
		log.Printf("Could not read directory %s: %s\n", path, err)
		return
	}

	var wg sync.WaitGroup
	for _, station := range stations {
		wg.Add(1)

		// Spawn separate task for each station directory
		go func(station os.DirEntry) {
			defer wg.Done()

			stnr, err := getStationNumber(station, config.Stations)
			if err != nil {
				// log.Println(err)
				return
			}

			stationDir := filepath.Join(path, station.Name())
			elements, err := os.ReadDir(stationDir)
			if err != nil {
				log.Printf("Could not read directory %s: %s\n", stationDir, err)
				return
			}

			for _, element := range elements {
				elemCode, err := getElementCode(element, config.Elements)
				if err != nil {
					// log.Println(err)
					continue
				}

				filename := filepath.Join(stationDir, element.Name())
				handle, err := os.Open(filename)
				if err != nil {
					log.Printf("Could not open file '%s': %s\n", filename, err)
					continue
				}

				timeseries, err := getTimeseries(ParamKey{table.TableName, elemCode}, stnr, pool, config)
				if err != nil {
					log.Printf("Error obtaining timeseries (%s, %v, %s): %s", table.TableName, stnr, elemCode, err)
					continue
				}

				data, err := parseData(handle, config.Sep, timeseries, table)
				if err != nil {
					log.Printf("Could not parse file '%s': %s\n", filename, err)
					continue
				}

				count, err := insertData(pool, data)
				if err != nil {
					log.Println("Failed bulk insertion:", err)
					continue
				}

				logStr := fmt.Sprintf("%v - %v - %v: %v/%v rows inserted", table.TableName, stnr, elemCode, count, len(data))
				if int(count) != len(data) {
					logStr = "WARN! " + logStr
				}
				log.Println(logStr)
			}

		}(station)
	}
	wg.Wait()

	log.SetOutput(os.Stdout)
	log.Println("Finished import of", table.TableName)
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
	elemCode := strings.TrimSuffix(element.Name(), ".csv")

	if elementList != nil && !slices.Contains(elementList, elemCode) {
		return "", errors.New(fmt.Sprintf("Element '%s' not in the list, skipping", elemCode))
	}

	if isNotValid(elemCode) {
		return "", errors.New(fmt.Sprintf("Element '%s' not set for import, skipping", elemCode))
	}

	return elemCode, nil
}

func parseData(handle io.ReadCloser, sep string, ts *Timeseries, table *TableInstructions) ([]ObsLARD, error) {
	defer handle.Close()
	scanner := bufio.NewScanner(handle)

	// Skip header
	scanner.Scan()

	// use relevant fromtime and totime from ElemTableName to avoid importing nonsense flags
	// TODO: what does this mean?
	// kdvhinfo, ok := TableTimeSeriesKDVH[fi.KDVHKeys]
	// if !ok {
	// that is okay though, orig_obstime is not defined in elem tables
	// log.Println("TimeSeries not recognized in KDVH cache", fi.KDVHKeys)
	// }

	var data []ObsLARD
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), sep)

		obsTime, err := time.Parse("2006-01-02_15:04:05", cols[0])
		if err != nil {
			return nil, err
		}

		// TODO: do we actually need to query KDVH directly or the proxy?
		// only import data between kdvh's defined fromtime and totime
		// if kdvhinfo != nil { if kdvhinfo.FromTime != nil && obsTime.Sub(*kdvhinfo.FromTime) < 0 { continue }
		// if kdvhinfo.ToTime != nil && obsTime.Sub(*kdvhinfo.ToTime) > 0 { break } }

		if obsTime.Year() >= table.ImportUntil {
			log.Println("Reached max import time for this table, inserting data")
			break
		}

		temp, err := table.DataFunction(
			ObsKDVH{
				Timeseries: ts,
				ObsTime:    obsTime,
				Data:       cols[1],
				Flags:      cols[2],
			},
		)
		if err != nil {
			return nil, err
		}

		data = append(data, temp)
	}

	return data, nil
}

func insertData(pool *pgxpool.Pool, data []ObsLARD) (int64, error) {
	// TODO: should this be a transaction?
	return pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"flags", "kdvh"},
		[]string{"timeseries", "obstime", "obsvalue", "controlinfo", "useinfo"},
		pgx.CopyFromSlice(len(data), func(i int) ([]any, error) {
			return []any{
				data[i].ID,
				data[i].ObsTime,
				data[i].CorrKDVH,
				data[i].KVFlagControlInfo,
				data[i].KVFlagUseInfo,
			}, nil
		}),
	)
}

func getTimeseries(key ParamKey, stnr int64, pool *pgxpool.Pool, config *ImportArgs) (*Timeseries, error) {
	var tsid int32

	offset := config.OffsetMap[key]
	meta, ok := config.MetadataMap[key]
	if !ok {
		return nil, errors.New("Missing metadata in Stinfosys")
	}

	// Query LARD labels table with stinfosys metadata
	err := pool.QueryRow(
		context.TODO(),
		`SELECT timeseries FROM labels.met
            WHERE station_id = $1
            AND param_id = $2
            AND type_id = $3
            AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
            AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))`,
		stnr, meta.ParamID, meta.TypeID, meta.Hlevel, meta.Sensor).Scan(&tsid)

	// If timeseries exists, return its ID and the metadata + offset
	if err == nil {
		return &Timeseries{tsid, offset, meta}, nil
	}

	// Otherwise insert new timeseries
	transaction, err := pool.Begin(context.TODO())
	if err != nil {
		return nil, err
	}

	err = transaction.QueryRow(
		context.TODO(),
		`INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id`,
		meta.Fromtime,
	).Scan(&tsid)
	if err != nil {
		return nil, err
	}

	_, err = transaction.Exec(
		context.TODO(),
		`INSERT INTO labels.met (timeseries, station_id, param_id, type_id, lvl, sensor)
            VALUES ($1, $2, $3, $4, $5, $6)`,
		tsid, stnr, meta.ParamID, meta.TypeID, meta.Hlevel, meta.Sensor)
	if err != nil {
		return nil, err
	}

	if err := transaction.Commit(context.TODO()); err != nil {
		return nil, err
	}

	return &Timeseries{tsid, offset, meta}, nil
}

// TODO: add CALL_SIGN?
var INVALID_ELEMENTS = []string{"TYPEID", "TAM_NORMAL_9120", "RRA_NORMAL_9120", "OT", "OTN", "OTX", "DD06", "DD12", "DD18"}

func isNotValid(element string) bool {
	return strings.Contains(element, "KOPI") || slices.Contains(INVALID_ELEMENTS, element)
}
