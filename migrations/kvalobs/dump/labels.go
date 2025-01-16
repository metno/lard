package dump

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	kvalobs "migrate/kvalobs/db"
	"migrate/utils"
)

type StationMap = map[int32][]*kvalobs.Label

func (db *Database) getLabels(table *Table, path string, pool *pgxpool.Pool, config *Config) (labels []*kvalobs.Label, err error) {
	// <base_path>/<db_name>/<table_name>/<timespan>/labels.csv
	labelFile := filepath.Join(path, "labels.csv")

	if _, err := os.Stat(labelFile); err != nil || config.UpdateLabels {
		fmt.Println("Fetching labels...")

		labels, err := dumpLabels(table, db, pool, config)
		if err != nil {
			return nil, err
		}
		return labels, WriteLabelCSV(labelFile, labels)
	}

	return ReadLabelCSV(labelFile)
}

// Builds a map of timeseries for each station id
func (db *Database) getStationLabelMap(table *Table, path string, pool *pgxpool.Pool, config *Config) (StationMap, error) {
	var labels []*kvalobs.Label
	var err error

	if config.LabelFile != "" {
		labels, err = config.loadLabels()
		if err != nil {
			return nil, err
		}
	} else {
		labels, err = db.getLabels(table, path, pool, config)
		if err != nil {
			return nil, err
		}
	}

	labelmap := make(map[int32][]*kvalobs.Label)
	for _, label := range labels {
		if !utils.IsNilOrContains(config.Stations, label.StationID) {
			continue
		}
		labelmap[label.StationID] = append(labelmap[label.StationID], label)
	}

	return labelmap, nil
}

func dumpLabels(table *Table, db *Database, pool *pgxpool.Pool, config *Config) ([]*kvalobs.Label, error) {
	slog.Info("Querying data labels...")
	// First query stationid and typeid from observations
	// Then query paramid, sensor, level from obsdata
	// This is faster than querying all of them together from data
	if err := db.InitUniqueStationsAndTypeIds(config.Timespan, pool); err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	// Channel used to send queried label slices
	// The main thread is responsible for merging them
	labelSets := make(chan []*kvalobs.Label)

	// Spawn task to retrieve label slices
	go table.DumpLabels(db, labelSets, pool, config)

	// TODO: maybe we can create the map directly here
	// TODO: should this directly write to the label file instead of concatenating stuff?
	var labels []*kvalobs.Label
	for set := range labelSets {
		labels = slices.Concat(labels, set)
	}

	slog.Info("Finished fetching labels!")
	return labels, nil
}

func dumpDataLabels(db *Database, sender chan []*kvalobs.Label, pool *pgxpool.Pool, config *Config) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, config.MaxConn)
	bar := utils.NewBar(len(db.UniqueStationTypes), "Dumping text labels...")

	for _, s := range db.UniqueStationTypes {
		wg.Add(1)
		semaphore <- struct{}{}

		go func() {
			defer func() {
				bar.Add(1)
				<-semaphore
				wg.Done()
			}()

			rows, err := pool.Query(
				context.TODO(),
				`SELECT DISTINCT paramid, sensor::int, level FROM obsdata
                    JOIN observations USING(observationid)
                    WHERE stationid = $1
                        AND typeid = $2
                        AND ($3::timestamp IS NULL OR obstime >= $3)
                        AND ($4::timestamp IS NULL OR obstime < $4)`,
				s.Stationid,
				s.Typeid,
				config.Timespan.From,
				config.Timespan.To,
			)
			if err != nil {
				slog.Error(err.Error())
				return
			}

			labels := make([]*kvalobs.Label, 0, rows.CommandTag().RowsAffected())
			labels, err = pgx.AppendRows(labels, rows, func(row pgx.CollectableRow) (*kvalobs.Label, error) {
				label := kvalobs.Label{StationID: s.Stationid, TypeID: s.Typeid}
				err := row.Scan(&(label.ParamID), &(label.Sensor), &(label.Level))
				return &label, err
			})

			if err != nil {
				slog.Error(err.Error())
				return
			}
			sender <- labels
		}()
	}
	wg.Wait()
	close(sender)
}

func dumpTextLabels(db *Database, sender chan []*kvalobs.Label, pool *pgxpool.Pool, config *Config) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, config.MaxConn)
	bar := utils.NewBar(len(db.UniqueStationTypes), "Dumping text labels...")

	for _, s := range db.UniqueStationTypes {
		wg.Add(1)
		semaphore <- struct{}{}

		go func() {
			defer func() {
				bar.Add(1)
				<-semaphore
				wg.Done()
			}()

			rows, err := pool.Query(
				context.TODO(),
				`SELECT DISTINCT paramid FROM obstextdata
                    JOIN observations USING(observationid)
                    WHERE stationid = $1
                        AND typeid = $2
                        AND ($3::timestamp IS NULL OR obstime >= $3)
                        AND ($4::timestamp IS NULL OR obstime < $4)`,
				s.Stationid,
				s.Typeid,
				config.Timespan.From,
				config.Timespan.To,
			)
			if err != nil {
				slog.Error(err.Error())
				return
			}

			labels := make([]*kvalobs.Label, 0, rows.CommandTag().RowsAffected())
			labels, err = pgx.AppendRows(labels, rows, func(row pgx.CollectableRow) (*kvalobs.Label, error) {
				label := kvalobs.Label{StationID: s.Stationid, TypeID: s.Typeid}
				err := row.Scan(&(label.ParamID))
				return &label, err
			})

			if err != nil {
				slog.Error(err.Error())
				return
			}
			sender <- labels
		}()
	}
	wg.Wait()
	close(sender)
}
