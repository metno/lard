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

	kvalobs "migrate/kvalobs/db"
	"migrate/utils"
)

func getLabels(table *kvalobs.Table, db *kvalobs.DB, pool *pgxpool.Pool, config *Config) (labels []*kvalobs.Label, err error) {
	if config.LabelFile != "" {
		return kvalobs.ReadLabelCSV(config.LabelFile)
	}

	// dumps/<db_name>/<table_name>/<timespan>/labels.csv
	labelFile := filepath.Join(table.Path, "labels.csv")

	if _, err := os.Stat(labelFile); err != nil || config.UpdateLabels {
		fmt.Println("Fetching labels...")
		labels, err = table.DumpLabels(config.Timespan, db, pool, config.MaxConn)
		if err != nil {
			return nil, err
		}
		return labels, kvalobs.WriteLabelCSV(labelFile, labels)
	}

	return kvalobs.ReadLabelCSV(labelFile)
}

// Given a slice of labels builds a map of timeseries for each station id
func getStationLabelMap(labels []*kvalobs.Label, config *Config) map[int32][]*kvalobs.Label {
	labelmap := make(map[int32][]*kvalobs.Label)

	for _, label := range labels {
		if !utils.IsNilOrContains(config.Stations, label.StationID) {
			continue
		}
		labelmap[label.StationID] = append(labelmap[label.StationID], label)
	}

	return labelmap
}

func dumpTable(table *kvalobs.Table, db *kvalobs.DB, pool *pgxpool.Pool, config *Config) {
	fmt.Printf("Dumping to %q...\n", table.Path)
	defer fmt.Println(strings.Repeat("- ", 40))

	labels, err := getLabels(table, db, pool, config)
	if err != nil || config.LabelsOnly {
		return
	}

	stationMap := getStationLabelMap(labels, config)

	// Used to limit connections to the database
	semaphore := make(chan struct{}, config.MaxConn)
	var wg sync.WaitGroup

	bar := utils.NewBar(len(stationMap), fmt.Sprintf("Dumping %s stations...", table.Name))
	bar.RenderBlank()

	for station, labels := range stationMap {
		stationPath := filepath.Join(table.Path, fmt.Sprint(station))
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

				if err := table.DumpSeries(label, config.Timespan, stationPath, pool); err != nil {
					slog.Error(label.LogStr() + err.Error())
					return
				}

				slog.Info(label.LogStr() + "dumped successfully")
			}()
		}
		wg.Wait()
		bar.Add(1)
	}
}

func dumpDB(database *kvalobs.DB, config *Config) {
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

		// dumps/<db_name>/<table_name>/<timespan>/
		table.SetPath(filepath.Join(
			config.Path,
			database.Name,
			table.Name,
			config.Timespan.ToDirName(),
		))
		if err := os.MkdirAll(table.Path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			return
		}

		if !config.LabelsOnly {
			// dumps/<db_name>/<table_name>/<timespan>/dump_<time_now>.log
			utils.SetLogFile(table.Path, "dump")
		}

		dumpTable(table, database, pool, config)
	}
}
