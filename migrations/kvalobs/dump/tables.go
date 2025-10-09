package dump

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	kvalobs "migrate/kvalobs/db"
	"migrate/utils"
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
	Tables     []*Table
	ConnEnvVar string
	// Stores unique (station ID, type ID) pairs, shared between `tables`
	UniqueStationTypes []*kvalobs.StationType
}

func (db *Database) InitUniqueStationsAndTypeIds(pool *pgxpool.Pool, config *Config) error {
	if db.UniqueStationTypes != nil {
		return nil
	}

	query := `SELECT DISTINCT stationid, typeid FROM observations
              WHERE ($1::timestamp IS NULL OR obstime >= $1)
                AND ($2::timestamp IS NULL OR obstime < $2)
				AND ($3::int[] IS NULL OR stationid = ANY($3))
				AND ($4::int[] IS NULL OR NOT stationid = ANY($4))
				AND ($5::int[] IS NULL OR typeid = ANY($5))
				AND ($6::int[] IS NULL OR NOT typeid = ANY($6))
			  ORDER BY stationid`

	fmt.Println("Fetching unique (station ID, type ID) pairs...")
	rows, err := pool.Query(context.TODO(),
		query, config.Timespan.From, config.Timespan.To, config.Stations, config.SkipStations, config.TypeIds, config.SkipTypeIds,
	)
	if err != nil {
		log.Error().Err(err).Msg("")
		return err
	}

	uniques := make([]*kvalobs.StationType, 0, rows.CommandTag().RowsAffected())
	db.UniqueStationTypes, err = pgx.AppendRows(uniques, rows, func(row pgx.CollectableRow) (*kvalobs.StationType, error) {
		var label kvalobs.StationType
		err := row.Scan(&label.Stationid, &label.Typeid)
		return &label, err
	})

	if err != nil {
		log.Error().Err(err).Msg("")
		return err
	}
	return nil
}

func initDumpDBs() []*Database {
	tables := []*Table{
		{Name: kvalobs.DataTableName, DumpLabels: dumpDataLabels, DumpSeries: dumpDataSeries},
		{Name: kvalobs.TextTableName, DumpLabels: dumpTextLabels, DumpSeries: dumpTextSeries},
	}

	return []*Database{
		{Name: kvalobs.KvDbName, ConnEnvVar: kvalobs.KvEnvVar, Tables: tables},
		{Name: kvalobs.HistDbName, ConnEnvVar: kvalobs.HistEnvVar, Tables: tables},
	}
}
