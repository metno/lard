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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickb777/period"
	"github.com/schollz/progressbar"

	"kdvh_importer/lard"
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
type Obs struct {
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

func (db *KDVH) Import(config *MigrateConfig) error {
	config.cacheMetadata(db)

	// Create connection pool for LARD
	pool, err := pgxpool.New(context.TODO(), os.Getenv("LARD_STRING"))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Lard:", err))
	}
	defer pool.Close()

	for _, table := range db.Tables {
		if config.Tables != nil && !slices.Contains(config.Tables, table.TableName) {
			continue
		}
		table.Import(pool, config)
	}

	return nil
}

func (table *KDVHTable) Import(pool *pgxpool.Pool, config *MigrateConfig) {
	defer utils.SendEmailOnPanic("importTable", config.Email)

	if table.ImportUntil == 0 {
		if config.Verbose {
			slog.Info("Skipping import of" + table.TableName + "  because this table is not set for import")
		}
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
			if config.Verbose {
				slog.Info(err.Error())
			}
			continue
		}

		stationDir := filepath.Join(path, station.Name())
		elements, err := os.ReadDir(stationDir)
		if err != nil {
			slog.Warn(fmt.Sprintf("Could not read directory %s: %s", stationDir, err))
			continue
		}

		var wg sync.WaitGroup
		bar := progressbar.New(len(elements))
		for _, element := range elements {
			bar.Add(1)

			elemCode, err := getElementCode(element, config.Elements)
			if err != nil {
				if config.Verbose {
					slog.Info(err.Error())
				}
				continue
			}
			filename := filepath.Join(stationDir, element.Name())

			wg.Add(1)
			go func() {
				defer wg.Done()
				logStr := fmt.Sprintf("%v - %v - %v: ", table.TableName, stnr, elemCode)

				file, err := os.Open(filename)
				if err != nil {
					slog.Warn(fmt.Sprintf("Could not open file '%s': %s", filename, err))
					return
				}
				defer file.Close()

				timeseries, err := getTimeseries(elemCode, table.TableName, stnr, pool, config)
				if err != nil {
					slog.Error(fmt.Sprintf("%v - %v - %v: could not obtain timeseries, %s", table.TableName, stnr, elemCode, err))
					slog.Error(logStr + "could not obtain timeseries - " + err.Error())
					return
				}

				parsedObs, err := parseData(file, timeseries, table, config)
				if err != nil {
					slog.Error(fmt.Sprintf("Could not parse data from '%s': %s", filename, err))
					return
				}

				// TODO: remove this and check if slice is empty inside inner function?
				if len(parsedObs.Flags) == 0 {
					slog.Info(logStr + "no rows to insert (all obstimes > max import time)")
					return
				}

				// Otherwise we need to insert to non-scalar table
				if !config.SkipData {
					if err := insertData(parsedObs.Data, pool, logStr); err != nil {
						slog.Error(fmt.Sprintf("%v - %v - %v: failed data bulk insertion, %s", table.TableName, stnr, elemCode, err))
					}

					if err := insertNonscalarData(parsedObs.NonScalar, pool, logStr); err != nil {
						slog.Error(fmt.Sprintf("%v - %v - %v: failed data bulk insertion, %s", table.TableName, stnr, elemCode, err))
					}
				}

				if !config.SkipFlags {
					if err := insertFlags(parsedObs.Flags, pool, logStr); err != nil {
						slog.Error(fmt.Sprintf("%v - %v - %v: failed flags bulk insertion, %s", table.TableName, stnr, elemCode, err))
					}
				}
			}()
		}
		wg.Wait()
	}

	log.SetOutput(os.Stdout)
	slog.Info(fmt.Sprint("Finished import of ", table.TableName))
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

func parseData(handle io.Reader, ts *TimeseriesInfo, table *KDVHTable, config *MigrateConfig) (*ParsedObs, error) {
	scanner := bufio.NewScanner(handle)

	// Skip header if present
	if config.HasHeader {
		scanner.Scan()
	}

	// TODO: is there a way we can get the number here? We should be able to get it when dumping?
	// There is a bit of data duplication but maybe for the better?
	// TODO: it doesn't make much sense to have these three arrays here, we are parsing a single element,
	// so we should already know if it's scalar or not :<
	data := make([]lard.Obs, 0, 1000)
	nonscalarData := make([]lard.NonScalarObs, 0, 1000)
	flags := make([]lard.Flags, 0, 1000)

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
			Obs{
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

		// TODO: should we do this or should we check from stinfosys?
		if temp.NonScalarData != nil {
			nonscalarData = append(nonscalarData, temp.toNonscalar())
		}

		// TODO: should this be an else after the above if?
		if temp.Data != nil {
			data = append(data, temp.toObs())
		}

		flags = append(flags, temp.toFlags())
	}

	return &ParsedObs{data, nonscalarData, flags}, nil
}

type ParsedObs struct {
	Data      []lard.Obs
	NonScalar []lard.NonScalarObs
	Flags     []lard.Flags
}

// TODO: benchmark double copyfrom vs batch insert?
func insertData(data []lard.Obs, pool *pgxpool.Pool, logStr string) error {
	if len(data) == 0 {
		return nil
	}

	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromSlice(len(data), func(i int) ([]any, error) {
			return []any{
				data[i].ID,
				data[i].ObsTime,
				data[i].Data,
			}, nil
		}),
	)
	if err != nil {
		return err
	}

	logStr += fmt.Sprintf("%v/%v data rows inserted", count, len(data))
	if int(count) != len(data) {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return nil
}

func insertNonscalarData(data []lard.NonScalarObs, pool *pgxpool.Pool, logStr string) error {
	if len(data) == 0 {
		return nil
	}

	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "nonscalar_data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromSlice(len(data), func(i int) ([]any, error) {
			return []any{
				data[i].ID,
				data[i].ObsTime,
				data[i].Data,
			}, nil
		}),
	)
	if err != nil {
		return err
	}

	logStr += fmt.Sprintf("%v/%v non-scalar data rows inserted", count, len(data))
	if int(count) != len(data) {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return nil

}

func insertFlags(flags []lard.Flags, pool *pgxpool.Pool, logStr string) error {
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"flags", "kdvh"},
		[]string{"timeseries", "obstime", "controlinfo", "useinfo"},
		pgx.CopyFromSlice(len(flags), func(i int) ([]any, error) {
			return []any{
				flags[i].ID,
				flags[i].ObsTime,
				flags[i].KVFlagControlInfo,
				flags[i].KVFlagUseInfo,
			}, nil
		}),
	)
	if err != nil {
		return err
	}

	logStr += fmt.Sprintf("%v/%v flags rows inserted", count, len(flags))
	if int(count) != len(flags) {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return nil
}

func getTimeseries(elemCode, tableName string, stnr int64, pool *pgxpool.Pool, config *MigrateConfig) (*TimeseriesInfo, error) {
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
