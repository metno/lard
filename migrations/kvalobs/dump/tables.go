package dump

import (
	"context"
	"fmt"
	"log/slog"
	kvalobs "migrate/kvalobs/db"
	"migrate/utils"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Table struct {
	Name       string
	DumpLabels LabelDumpFunc // Function that dumps labels from the table
	DumpSeries ObsDumpFunc   // Function that dumps observations from the table
}

// Given the db.UniqueStationTypes queries the table for all the matching labels and send them
// through the sets channel for concurrent processing
type LabelDumpFunc func(db *Database, sets chan []*kvalobs.Label, pool *pgxpool.Pool, config *Config)

// Function used to query timeseries from kvalobs for a specific label and dump them inside path
type ObsDumpFunc func(label *kvalobs.Label, timespan *utils.TimeSpan, path string, pool *pgxpool.Pool) error

type Database struct {
	Name       string
	Tables     map[string]*Table
	ConnEnvVar string
	// Stores unique (station ID, type ID) pairs, shared between `tables`
	UniqueStationTypes []*kvalobs.StationType
}

func (db *Database) InitUniqueStationsAndTypeIds(timespan *utils.TimeSpan, pool *pgxpool.Pool) error {
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
		slog.Error(err.Error())
		return err
	}

	uniques := make([]*kvalobs.StationType, 0, rows.CommandTag().RowsAffected())
	db.UniqueStationTypes, err = pgx.AppendRows(uniques, rows, func(row pgx.CollectableRow) (*kvalobs.StationType, error) {
		var label kvalobs.StationType
		err := row.Scan(&label.Stationid, &label.Typeid)
		return &label, err
	})

	if err != nil {
		slog.Error(err.Error())
		return err
	}
	return nil
}

func initDumpDBs() map[string]*Database {
	tables := map[string]*Table{
		kvalobs.DataTableName: {Name: kvalobs.DataTableName, DumpLabels: dumpDataLabels, DumpSeries: dumpDataSeries},
		kvalobs.TextTableName: {Name: kvalobs.TextTableName, DumpLabels: dumpTextLabels, DumpSeries: dumpTextSeries},
	}

	return map[string]*Database{
		kvalobs.KvDbName:   {Name: kvalobs.KvDbName, ConnEnvVar: kvalobs.KvEnvVar, Tables: tables},
		kvalobs.HistDbName: {Name: kvalobs.HistDbName, ConnEnvVar: kvalobs.HistEnvVar, Tables: tables},
	}
}
