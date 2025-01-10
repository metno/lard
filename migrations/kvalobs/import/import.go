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
	"migrate/kvalobs/import/cache"
	"migrate/lard"
	"migrate/utils"
)

// NOTE: we return the number of inserted rows for the tests
func ImportTable(table *kvalobs.Table, cache *cache.Cache, pool *pgxpool.Pool, config *Config) (int64, error) {
	fmt.Printf("Importing from %q...\n", table.Path)
	defer fmt.Println(strings.Repeat("- ", 40))

	stations, err := os.ReadDir(table.Path)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}

	// Used to limit number of spawned threads
	// Too many threads can lead to an OOM kill, due to slice allocations in parseData
	semaphore := make(chan struct{}, config.MaxWorkers)

	fmt.Printf("Number of stations to import: %d...\n", len(stations))
	var rowsInserted int64
	for _, station := range stations {
		stnr, err := strconv.ParseInt(station.Name(), 10, 32)
		if err != nil || !utils.IsNilOrContains(config.Stations, int32(stnr)) {
			continue
		}

		stationDir := filepath.Join(table.Path, station.Name())
		labels, err := os.ReadDir(stationDir)
		if err != nil {
			slog.Warn(err.Error())
			continue
		}

		bar := utils.NewBar(len(labels), fmt.Sprintf("%10s", station.Name()))
		bar.RenderBlank()

		var wg sync.WaitGroup
		for _, file := range labels {
			semaphore <- struct{}{}
			wg.Add(1)

			go func() {
				defer func() {
					bar.Add(1)
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
				// Check if data for this station/element is restricted
				if !cache.TimeseriesIsOpen(label.StationID, label.TypeID, label.ParamID) {
					// TODO: eventually use this to choose which table to use on insert
					slog.Warn(logStr + "timeseries data is restricted, skipping")
					return
				}

				tsTimespan, err := cache.GetSeriesTimespan(label)
				if err != nil {
					slog.Error(logStr + err.Error())
					return
				}

				// TODO: figure out where to get fromtime, kvalobs directly? Stinfosys?
				tsid, err := lard.GetTimeseriesID(label.ToLard(), tsTimespan, pool)
				if err != nil {
					slog.Error(logStr + err.Error())
					return
				}

				filename := filepath.Join(stationDir, file.Name())
				count, err := table.Import(tsid, label, filename, logStr, pool)
				if err != nil {
					// Logged inside table.Import
					return
				}

				rowsInserted += count
			}()
		}
		wg.Wait()
	}

	outputStr := fmt.Sprintf("%v: %v total rows inserted", table.Path, rowsInserted)
	slog.Info(outputStr)
	fmt.Println(outputStr)

	return rowsInserted, nil
}

func ImportAllTimespans(table *kvalobs.Table, cache *cache.Cache, pool *pgxpool.Pool, config *Config) (int64, error) {
	timespans, err := os.ReadDir(table.Path)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}

	path := table.Path
	for _, span := range timespans {
		if !span.IsDir() {
			continue
		}

		table.SetPath(filepath.Join(path, span.Name()))
		ImportTable(table, cache, pool, config)
	}

	return 0, nil
}

func ImportDB(database kvalobs.DB, cache *cache.Cache, pool *pgxpool.Pool, config *Config) {
	for name, table := range database.Tables {
		if !utils.StringIsEmptyOrEqual(config.Table, name) {
			continue
		}

		// dumps/<db_name>/<table_name>/(<SpanDir>/)
		table.SetPath(filepath.Join(
			config.Path,
			database.Name,
			table.Name,
			config.SpanDir,
		))
		// dumps/<db_name>/<table_name>/<timespan>/import_<now>.log
		utils.SetLogFile(config.Path, "import")

		if config.SpanDir == "" {
			ImportAllTimespans(table, cache, pool, config)
		} else {
			ImportTable(table, cache, pool, config)
		}
	}
}
