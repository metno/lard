package db

import (
	"context"
	"fmt"
	"log/slog"
	"migrate/utils"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const OBSDATA_QUERY string = `SELECT DISTINCT paramid, sensor::int, level FROM obsdata
JOIN observations USING(observationid)
WHERE stationid = $1
  AND typeid = $2
  AND ($3::timestamp IS NULL OR obstime >= $3)
  AND ($4::timestamp IS NULL OR obstime < $4)`

const OBSTEXTDATA_QUERY string = `SELECT DISTINCT paramid FROM obstextdata
JOIN observations USING(observationid)
WHERE stationid = $1
  AND typeid = $2
  AND ($3::timestamp IS NULL OR obstime >= $3)
  AND ($4::timestamp IS NULL OR obstime < $4)`

type StationType struct {
	stationid int32
	typeid    int32
}

type TableLabelInfo struct {
	msg      string
	query    string
	scanner  labelScanner
	timespan *utils.TimeSpan
}

type labelScanner = func(row pgx.CollectableRow, label *Label) (*Label, error)

func ScanTextLabel(row pgx.CollectableRow, label *Label) (*Label, error) {
	err := row.Scan(&(label.ParamID))
	return label, err
}

func ScanDataLabel(row pgx.CollectableRow, label *Label) (*Label, error) {
	err := row.Scan(&(label.ParamID), &(label.Sensor), &(label.Level))
	return label, err
}

func (db *DB) initUniqueStationsAndTypeIds(timespan *utils.TimeSpan, pool *pgxpool.Pool) error {
	if db.UniqueStationTypes != nil {
		return nil
	}

	fmt.Println("Fetching unique (station ID, type ID) pairs...")
	rows, err := pool.Query(context.TODO(),
		`SELECT DISTINCT stationid, typeid FROM observations
            WHERE ($1::timestamp IS NULL OR obstime >= $1)
              AND ($2::timestamp IS NULL OR obstime < $2)
            ORDER BY stationid`,
		timespan.From, timespan.To)
	if err != nil {
		return err
	}

	uniques := make([]*StationType, 0, rows.CommandTag().RowsAffected())
	db.UniqueStationTypes, err = pgx.AppendRows(uniques, rows, func(row pgx.CollectableRow) (*StationType, error) {
		var label StationType
		err := row.Scan(&label.stationid, &label.typeid)
		return &label, err
	})

	if err != nil {
		return err
	}
	return nil
}

// TODO: quite annoying, this should be able to take a config struct directly
func dumpDataLabels(timespan *utils.TimeSpan, db *DB, pool *pgxpool.Pool, maxConn int) ([]*Label, error) {
	// First query stationid and typeid from observations
	// Then query paramid, sensor, level from obsdata
	// This is faster than querying all of them together from data
	slog.Info("Querying data labels...")
	if err := db.initUniqueStationsAndTypeIds(timespan, pool); err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	// Channel used to send queried labels
	// The main thread is responsible for merging them
	c := make(chan []*Label)

	go db.retrieveLabels(
		TableLabelInfo{
			msg:      "Dumping data labels...   ",
			query:    OBSDATA_QUERY,
			scanner:  ScanDataLabel,
			timespan: timespan,
		},
		c,
		maxConn,
		pool,
	)

	// TODO: maybe we can create the map directly here
	// TODO: this should probably directly write to the label file instead of concatenating stuff
	var labels []*Label
	for received := range c {
		labels = slices.Concat(labels, received)
	}

	slog.Info("Finished fetching labels!")
	return labels, nil
}

func dumpTextLabels(timespan *utils.TimeSpan, db *DB, pool *pgxpool.Pool, maxConn int) ([]*Label, error) {
	// First query stationid and typeid from observations
	// Then query paramid from obstextdata
	// This is faster than querying all of them together from data
	slog.Info("Querying text labels...")
	if err := db.initUniqueStationsAndTypeIds(timespan, pool); err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	// Channel used to send queried labels
	// The main thread is responsible for merging them
	c := make(chan []*Label)

	go db.retrieveLabels(
		TableLabelInfo{
			msg:      "Dumping text labels...   ",
			query:    OBSTEXTDATA_QUERY,
			scanner:  ScanTextLabel,
			timespan: timespan,
		},
		c,
		maxConn,
		pool,
	)

	// TODO: this should probably directly write to the label file instead of concatenating stuff
	var labels []*Label
	for received := range c {
		labels = slices.Concat(labels, received)
	}

	slog.Info("Finished fetching labels!")
	return labels, nil
}

func (db *DB) retrieveLabels(info TableLabelInfo, sender chan []*Label, maxConn int, pool *pgxpool.Pool) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, maxConn)
	bar := utils.NewBar(len(db.UniqueStationTypes), info.msg)

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
				info.query,
				s.stationid,
				s.typeid,
				info.timespan.From,
				info.timespan.To,
			)
			if err != nil {
				slog.Error(err.Error())
				return
			}

			labels := make([]*Label, 0, rows.CommandTag().RowsAffected())
			labels, err = pgx.AppendRows(labels, rows, func(row pgx.CollectableRow) (*Label, error) {
				label := Label{StationID: s.stationid, TypeID: s.typeid}
				return info.scanner(row, &label)
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
