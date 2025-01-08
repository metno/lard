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

func getLabels(table *kvalobs.Table, pool *pgxpool.Pool, timespan *utils.TimeSpan, config *Config) (labels []*kvalobs.Label, err error) {
	labelFile := fmt.Sprintf("%s_labels_%s.csv", table.Path, timespan.ToString())

	if _, err := os.Stat(labelFile); err != nil || config.UpdateLabels {
		labels, err = table.DumpLabels(timespan, pool, config.MaxConn)
		if err != nil {
			return nil, err
		}
		return labels, kvalobs.WriteLabelCSV(labelFile, labels)
	}
	return kvalobs.ReadLabelCSV(labelFile)
}

func getStationLabelMap(labels []*kvalobs.Label) map[int32][]*kvalobs.Label {
	labelmap := make(map[int32][]*kvalobs.Label)

	for _, label := range labels {
		labelmap[label.StationID] = append(labelmap[label.StationID], label)
	}

	return labelmap
}

func dumpTable(table *kvalobs.Table, pool *pgxpool.Pool, config *Config) {
	if !config.LabelsOnly {
		utils.SetLogFile(table.Path, "dump")
	}
	fmt.Printf("Dumping to %q...\n", table.Path)
	defer fmt.Println(strings.Repeat("- ", 40))

	timespan := config.TimeSpan()
	labels, err := getLabels(table, pool, timespan, config)
	if err != nil || config.LabelsOnly {
		return
	}

	stationMap := getStationLabelMap(labels)

	// Used to limit connections to the database
	semaphore := make(chan struct{}, config.MaxConn)
	var wg sync.WaitGroup

	for station, labels := range stationMap {
		stationPath := filepath.Join(table.Path, fmt.Sprint(station))

		if !utils.IsEmptyOrContains(config.Stations, station) {
			continue
		}

		if err := os.MkdirAll(stationPath, os.ModePerm); err != nil {
			slog.Error(err.Error())
			return
		}

		// TODO: this bar is a bit deceiving if you don't dump all the labels
		// Maybe should only cache the ones requested from cli?
		bar := utils.NewBar(len(labels), fmt.Sprintf("%10d", station))
		bar.RenderBlank()

		for _, label := range labels {
			wg.Add(1)
			semaphore <- struct{}{}

			go func() {
				defer func() {
					bar.Add(1)
					wg.Done()
					// Release semaphore
					<-semaphore
				}()

				if !config.ShouldProcessLabel(label) {
					return
				}

				logStr := label.LogStr()
				if err := table.DumpSeries(label, timespan, stationPath, pool); err != nil {
					slog.Info(logStr + err.Error())
					return
				}

				slog.Info(logStr + "dumped successfully")
			}()
		}
		wg.Wait()
	}
}

func dumpDB(database kvalobs.DB, config *Config) {
	pool, err := pgxpool.New(context.Background(), os.Getenv(database.ConnEnvVar))
	if err != nil {
		slog.Error(fmt.Sprint("Could not connect to Kvalobs:", err))
		return
	}
	defer pool.Close()

	path := filepath.Join(config.Path, database.Name)
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		slog.Error(err.Error())
		return
	}

	for name, table := range database.Tables {
		if !utils.StringIsEmptyOrEqual(config.Table, name) {
			continue
		}

		table.Path = filepath.Join(path, table.Name)
		dumpTable(table, pool, config)
	}
}
