package port

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	kvalobs "migrate/kvalobs/db"
	"migrate/lard"
	"migrate/utils"
)

// NOTE: we return the number of inserted rows for the tests
func (table *Table) Import(path string, cache *Cache, pool *pgxpool.Pool, config *Config) (int64, error) {
	fmt.Printf("Importing from %q...\n", path)
	defer fmt.Println(strings.Repeat("- ", 40))

	stations, err := os.ReadDir(path)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}

	// Used to limit number of spawned threads
	// Too many threads can lead to an OOM kill, due to slice allocations in table.Import
	semaphore := make(chan struct{}, config.MaxWorkers)
	bar := utils.NewBar(len(stations), fmt.Sprintf("Importing %s stations...", table.Name))
	bar.RenderBlank()

	var rowsInserted int64
	for _, station := range stations {
		stnr, err := strconv.ParseInt(station.Name(), 10, 32)
		if err != nil || !utils.IsNilOrContains(config.Stations, int32(stnr)) {
			bar.Add(1)
			continue
		}

		stationDir := filepath.Join(path, station.Name())
		labels, err := os.ReadDir(stationDir)
		if err != nil {
			slog.Warn(err.Error())
			bar.Add(1)
			continue
		}

		var wg sync.WaitGroup
		for _, file := range labels {
			semaphore <- struct{}{}
			wg.Add(1)

			go func() {
				defer func() {
					<-semaphore
					wg.Done()
				}()

				label, err := kvalobs.LabelFromFilename(file.Name())
				if err != nil {
					slog.Error(err.Error())
					return
				}

				if !config.ShouldProcessLabel(label) {
					return
				}

				logStr := label.LogStr()
				tsid, err := getTsid(label, cache, pool)
				if err != nil {
					slog.Error(logStr + err.Error())
				}

				filename := filepath.Join(stationDir, file.Name())
				file, err := os.Open(filename)
				if err != nil {
					slog.Error(logStr + err.Error())
					return
				}
				defer file.Close()

				count, err := table.ImportFn(file, tsid, label, logStr, pool)
				if err == nil {
					rowsInserted += count
				}
			}()
		}
		wg.Wait()
		bar.Add(1)
	}

	outputStr := fmt.Sprintf("%v: %v total rows inserted", path, rowsInserted)
	slog.Info(outputStr)
	fmt.Println(outputStr)

	return rowsInserted, nil
}

func getTsid(label *kvalobs.Label, cache *Cache, pool *pgxpool.Pool) (int64, error) {
	// Check if data for this station/element is restricted
	if !cache.TimeseriesIsOpen(label.StationID, label.TypeID, label.ParamID) {
		// TODO: eventually use this to choose which table to use on insert
		return 0, fmt.Errorf("timeseries data is restricted, skipping")
	}

	tsTimespan, err := cache.GetSeriesTimespan(label)
	if err != nil {
		return 0, err
	}

	// TODO: figure out where to get fromtime, kvalobs directly? Stinfosys?
	tsid, err := lard.GetTimeseriesID(label.ToLard(), tsTimespan, pool)
	if err != nil {
		return 0, err
	}

	return tsid, nil
}

func (table *Table) ImportAllTimespans(path string, cache *Cache, pool *pgxpool.Pool, config *Config) (int64, error) {
	timespans, err := os.ReadDir(path)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}

	for _, span := range timespans {
		if !span.IsDir() {
			continue
		}

		table.Import(filepath.Join(path, span.Name()), cache, pool, config)
	}

	return 0, nil
}

func (db *Database) Import(cache *Cache, pool *pgxpool.Pool, config *Config) {
	for name, table := range db.Tables {
		if !utils.StringIsEmptyOrEqual(config.Table, name) {
			continue
		}

		// <base_path>/<db_name>/<table_name>/<timespan>/
		path := filepath.Join(
			config.Path,
			db.Name,
			table.Name,
			config.SpanDir,
		)

		logFile := db.Name + "_" + table.Name
		handle := utils.SetLogFile(logFile, "import")
		defer handle.Close()

		if config.SpanDir == "" {
			table.ImportAllTimespans(path, cache, pool, config)
		} else {
			table.Import(path, cache, pool, config)
		}
	}
}
