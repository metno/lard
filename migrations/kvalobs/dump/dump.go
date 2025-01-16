package dump

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/utils"
)

func (table *Table) dump(stations StationMap, path string, pool *pgxpool.Pool, config *Config) {
	fmt.Printf("Dumping to %q...\n", path)
	defer fmt.Println(strings.Repeat("- ", 40))

	// Used to limit connections to the database
	semaphore := make(chan struct{}, config.MaxConn)
	var wg sync.WaitGroup

	// TODO: misleading if using a separate dump file
	// maybe should use a bar without a set number of items
	// But we can always filter the logs afterwards
	bar := utils.NewBar(len(stations), "Dumping stations...")
	bar.RenderBlank()

	for station, labels := range stations {
		stationPath := filepath.Join(path, fmt.Sprint(station))
		if err := os.MkdirAll(stationPath, os.ModePerm); err != nil {
			slog.Error(err.Error())
			return
		}

		for _, label := range labels {
			wg.Add(1)
			semaphore <- struct{}{}

			go func() {
				defer func() {
					<-semaphore
					wg.Done()
				}()

				if !config.ShouldProcessLabel(label) {
					return
				}

				if err := table.DumpSeries(label, config.Timespan, stationPath, pool); err == nil {
					slog.Info(label.LogStr() + "dumped successfully")
				}
			}()
		}
		wg.Wait()
		bar.Add(1)
	}
}

func (database *Database) dump(config *Config) {
	pool, err := pgxpool.New(context.Background(), os.Getenv(database.ConnEnvVar))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
		return
	}
	defer pool.Close()

	for name, table := range database.Tables {
		if !utils.StringIsEmptyOrEqual(config.Table, name) {
			continue
		}

		// ._<db_name>_<table_name>_<timespan>_<utc_now>_dump.log
		logFile := strings.Join([]string{database.Name, table.Name, config.Timespan.ToDirName()}, "_")
		handle := utils.SetLogFile(logFile, "dump")
		defer handle.Close()

		path := filepath.Join(
			config.Path,
			database.Name,
			table.Name,
			config.Timespan.ToDirName(),
		)
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			return
		}

		stations, err := database.getStationLabelMap(table, path, pool, config)
		if err != nil || config.LabelsOnly {
			return
		}

		table.dump(stations, path, pool, config)
	}
}
