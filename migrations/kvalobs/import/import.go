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

func ImportTable(table *kvalobs.Table, cache *cache.Cache, pool *pgxpool.Pool, config *Config) (int64, error) {
	fmt.Printf("Importing from %q...\n", table.Path)
	defer fmt.Println(strings.Repeat("- ", 40))

	stations, err := os.ReadDir(table.Path)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}

	importTimespan := config.TimeSpan()
	fmt.Printf("Number of stations to import: %d...\n", len(stations))
	var rowsInserted int64
	for _, station := range stations {
		stnr, err := strconv.ParseInt(station.Name(), 10, 32)
		if err != nil || !utils.IsEmptyOrContains(config.Stations, int32(stnr)) {
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
			wg.Add(1)
			go func() {
				defer func() {
					bar.Add(1)
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
				// TODO: it's probably better to dump in different directories
				// instead of introducing runtime checks
				count, err := table.Import(tsid, label, filename, logStr, importTimespan, pool)
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

func ImportDB(database kvalobs.DB, cache *cache.Cache, pool *pgxpool.Pool, config *Config) {
	path := filepath.Join(config.Path, database.Name)

	for name, table := range database.Tables {
		if !utils.StringIsEmptyOrEqual(config.Table, name) {
			continue
		}

		table.Path = filepath.Join(path, table.Name)
		utils.SetLogFile(table.Path, "import")
		ImportTable(table, cache, pool, config)
	}
}
