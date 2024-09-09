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

type MetaKDVH struct {
	TableName string
	Station   int64
	ElemCode  string
	FromTime  *time.Time
	ToTime    *time.Time
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

type ImportConfig struct {
	BaseDir     string   `long:"dir" required:"true" description:"Base directory where the dumped data is stored"`
	Sep         string   `long:"sep" default:";"  description:"Separator character in the dumped files"`
	TablesCmd   string   `long:"table" default:"" description:"Optional comma separated list of table names. By default all available tables are processed"`
	StationsCmd string   `long:"station" default:"" description:"Optional comma separated list of stations IDs. By default all station IDs are processed"`
	ElementsCmd string   `long:"elemcode" default:"" description:"Optional comma separated list of element codes. By default all element codes are processed"`
	Header      bool     `long:"header" description:"Add this flag if the dumped files have a header row"`
	Email       []string `long:"email" description:"Optional email address used to notify if the program crashed"`
	Tables      []string
	Stations    []string
	Elements    []string
	OffsetMap   map[ParamKey]period.Period // Map of offsets used to correct (?) KDVH times for specific parameters
	StinfoMap   map[ParamKey]Metadata      // Map of metadata used to query timeseries id in LARD
	KDVHMap     map[KDVHKey]*MetaKDVH      // Map of from_time and to_time for each (table, station, element) triplet
}

// Updates config:
// 0. Checks validity of separator
// 1. Populates slices by parsing the string provided via cmd
// 2. Caches time offsets by reading 'param_offset.csv'
// 3. Caches metadata from Stinfosys
// 4. Caches metadata from KDVH proxy
func (config *ImportConfig) setup() {
	if len(config.Sep) > 1 {
		log.Println("--sep= accepts only single characters. Defaulting to ';'")
		config.Sep = ";"
	}

	if config.TablesCmd != "" {
		config.Tables = strings.Split(config.TablesCmd, ",")
	}
	if config.StationsCmd != "" {
		config.Stations = strings.Split(config.StationsCmd, ",")
	}
	if config.ElementsCmd != "" {
		config.Elements = strings.Split(config.ElementsCmd, ",")
	}

	config.OffsetMap = cacheParamOffsets()
	config.StinfoMap = cacheStinfo(config.Tables, config.Elements)
	config.KDVHMap = cacheKDVH(config.Tables, config.Stations, config.Elements)
}

// This method is automatically called by go-flags while parsing the cmd
func (config *ImportConfig) Execute(_ []string) error {
	config.setup()

	// Create connection pool for LARD
	pool, err := pgxpool.New(context.TODO(), os.Getenv("LARD_STRING"))
	if err != nil {
		log.Fatalln("Could not connect to Lard:", err)
	}
	defer pool.Close()

	for _, table := range TABLE2INSTRUCTIONS {
		if config.Tables != nil && !slices.Contains(config.Tables, table.TableName) {
			continue
		}

		table.updateDefaults()

		importTable(pool, table, config)
	}

	return nil
}

// Save metadata for later use by quering Stinfosys
func cacheStinfo(tables, elements []string) map[ParamKey]Metadata {
	cache := make(map[ParamKey]Metadata)

	log.Println("Connecting to Stinfosys to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("STINFO_STRING"))
	if err != nil {
		log.Fatalln("Could not connect to Stinfosys. Make sure to be connected to the VPN.", err)
	}
	defer conn.Close(context.TODO())

	for _, table := range TABLE2INSTRUCTIONS {
		if tables != nil && !slices.Contains(tables, table.TableName) {
			continue
		}

		query := `SELECT elem_code, table_name, typeid, paramid, hlevel, sensor, fromtime
                    FROM elem_map_cfnames_param
                    WHERE table_name = $1
                    AND ($2::text[] IS NULL OR elem_code = ANY($2))`

		rows, err := conn.Query(context.TODO(), query, table.TableName, elements)
		if err != nil {
			log.Fatalln(err)
		}

		// TODO: eventually move to RowToStructByName (less brittle, but requires adding tags to the struct)
		metas, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Metadata])
		if err != nil {
			log.Fatalln(err)
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

	log.Println("Connecting to KDVH proxy to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv("KDVH_PROXY_CONN"))
	if err != nil {
		log.Fatalln("Could not connect to KDVH proxy. Make sure to be connected to the VPN.", err)
	}
	defer conn.Close(context.TODO())

	for _, t := range TABLE2INSTRUCTIONS {
		if tables != nil && !slices.Contains(tables, t.TableName) {
			continue
		}

		// TODO: probably need to sanitize these inputs
		// TODO: stations should be []int64 here?
		query := fmt.Sprintf(
			`SELECT table_name, stnr, elem_code, fdato, tdato FROM %s
                WHERE ($1::bigint[] IS NULL OR stnr = ANY($1))
                AND ($2::text[] IS NULL OR elem_code = ANY($2))`,
			t.ElemTableName,
		)

		rows, err := conn.Query(context.TODO(), query, stations, elements)
		if err != nil {
			log.Fatalln(err)
		}

		metas, err := pgx.CollectRows(rows, pgx.RowToStructByPos[MetaKDVH])
		if err != nil {
			log.Fatalln(err)
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

func importTable(pool *pgxpool.Pool, table *TableInstructions, config *ImportConfig) {
	defer sendEmailOnPanic("importTable", config.Email)

	if table.ImportUntil == 0 {
		// log.Printf("Skipping import of %s because this table is not set for import", table.TableName)
		return
	}

	log.Println("Starting import of", table.TableName)
	setLogFile(table.TableName, "import")

	path := filepath.Join(config.BaseDir, table.TableName+"_combined")
	stations, err := os.ReadDir(path)
	if err != nil {
		log.Printf("Could not read directory %s: %s", path, err)
		return
	}

	for _, station := range stations {
		stnr, err := getStationNumber(station, config.Stations)
		if err != nil {
			// log.Println(err)
			return
		}

		stationDir := filepath.Join(path, station.Name())
		elements, err := os.ReadDir(stationDir)
		if err != nil {
			log.Printf("Could not read directory %s: %s", stationDir, err)
			return
		}

		var wg sync.WaitGroup
		for _, element := range elements {
			elemCode, err := getElementCode(element, config.Elements)
			if err != nil {
				continue
			}
			filename := filepath.Join(stationDir, element.Name())

			wg.Add(1)
			go func() {
				defer wg.Done()

				handle, err := os.Open(filename)
				if err != nil {
					log.Printf("Could not open file '%s': %s", filename, err)
					return
				}
				defer handle.Close()

				timeseries, err := getTimeseries(elemCode, table.TableName, stnr, pool, config)
				if err != nil {
					log.Printf("Error obtaining timeseries (%s, %v, %s): %s", table.TableName, stnr, elemCode, err)
					return
				}

				data, err := parseData(handle, timeseries, table, config)
				if err != nil {
					log.Printf("Could not parse file '%s': %s", filename, err)
					return
				}

				if len(data) == 0 {
					log.Printf("%v - %v - %v: no rows to insert (all obstimes > max import time)", table.TableName, stnr, elemCode)
					return
				}

				count, err := insertData(pool, data)
				if err != nil {
					log.Printf("Failed bulk insertion (%s, %v, %s): %s", table.TableName, stnr, elemCode, err)
					return
				}

				logStr := fmt.Sprintf("%v - %v - %v: %v/%v rows inserted", table.TableName, stnr, elemCode, count, len(data))
				if int(count) != len(data) {
					logStr = "WARN! " + logStr
				}
				log.Println(logStr)
			}()
		}
		wg.Wait()
	}

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
	elemCode := strings.ToUpper(strings.TrimSuffix(element.Name(), ".csv"))

	if elementList != nil && !slices.Contains(elementList, elemCode) {
		return "", errors.New(fmt.Sprintf("Element '%s' not in the list, skipping", elemCode))
	}

	if elemcodeIsInvalid(elemCode) {
		return "", errors.New(fmt.Sprintf("Element '%s' not set for import, skipping", elemCode))
	}

	// TODO: make sure elemCode has the right casing
	return elemCode, nil
}

func parseData(handle io.Reader, ts *TimeseriesInfo, table *TableInstructions, config *ImportConfig) ([]ObsLARD, error) {
	scanner := bufio.NewScanner(handle)

	// Skip header if present
	if config.Header {
		scanner.Scan()
	}

	// TODO: maybe use csv.Reader
	var data []ObsLARD
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), config.Sep)

		obsTime, err := time.Parse("2006-01-02_15:04:05", cols[0])
		if err != nil {
			return nil, err
		}

		// TODO: not sure why we need this?
		// only import data between kdvh's defined fromtime and totime
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

func getTimeseries(elemCode, tableName string, stnr int64, pool *pgxpool.Pool, config *ImportConfig) (*TimeseriesInfo, error) {
	var tsid int32
	key := ParamKey{elemCode, tableName}

	offset := config.OffsetMap[key]
	stinfoMeta, ok := config.StinfoMap[key]
	if !ok {
		// TODO: should it fail here?
		return nil, errors.New("Missing metadata in Stinfosys")
	}

	// if there's no metadata in KDVH we insert anyway (hopefully it's in Stinfosys)
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

// TODO: add CALL_SIGN? It's not in stinfosys
var INVALID_ELEMENTS = []string{"TYPEID", "TAM_NORMAL_9120", "RRA_NORMAL_9120", "OT", "OTN", "OTX", "DD06", "DD12", "DD18"}

func elemcodeIsInvalid(element string) bool {
	return strings.Contains(element, "KOPI") || slices.Contains(INVALID_ELEMENTS, element)
}
