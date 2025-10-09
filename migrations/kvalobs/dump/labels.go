package dump

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	// "os"
	// "path/filepath"
	// "slices"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	kvalobs "migrate/kvalobs/db"
	"migrate/utils"
)

type StationMap = map[kvalobs.StationType][]*kvalobs.Label

func getStationLabelMap(table *Table, db *Database, path string, pool *pgxpool.Pool, config *Config) (StationMap, error) {
	fmt.Println("Fetching labels...")
	log.Info().Msg("Fetching labels......")

	// First query stationid and typeid from observations
	// Then query paramid, sensor, level from obsdata
	// This seems to be faster than querying all of them together from data
	if err := db.InitUniqueStationsAndTypeIds(pool, config); err != nil {
		log.Error().Err(err).Msg("")
		return nil, err
	}

	// Channel used to send queried label slices
	// The main thread is responsible for merging them
	labelSets := make(chan []*kvalobs.Label)

	// Spawn task to retrieve label slices
	go table.DumpLabels(db, labelSets, pool, config)

	labelMap := make(map[kvalobs.StationType][]*kvalobs.Label)

	// Each set carries the labels for a single (stationid, typeid) pair
	for set := range labelSets {
		if len(set) == 0 {
			continue
		}

		key := kvalobs.StationType{
			Stationid: set[0].StationID,
			Typeid:    set[0].TypeID,
		}
		labelMap[key] = set
	}

	filename := filepath.Join(path, fmt.Sprintf("labels_%s.csv", time.Now().Format(time.RFC3339)))

	log.Info().Msg("Finished fetching labels!")
	return labelMap, kvalobs.WriteLabelCSV(filename, labelMap)
}

func dumpDataLabels(db *Database, sender chan []*kvalobs.Label, pool *pgxpool.Pool, config *Config) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, config.MaxConn)
	bar := utils.NewBar(len(db.UniqueStationTypes), "Dumping data labels...", config.Test)

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
                        AND ($4::timestamp IS NULL OR obstime < $4)
						AND ($5::int[] IS NULL OR paramid = ANY($5))
						AND ($6::int[] IS NULL OR NOT paramid = ANY($6))
						AND ($7::int[] IS NULL OR sensor::int = ANY($7))
						AND ($8::int[] IS NULL OR NOT sensor::int = ANY($8))
						AND ($9::int[] IS NULL OR level = ANY($9))
						AND ($10::int[] IS NULL OR NOT level = ANY($10))`,
				s.Stationid,
				s.Typeid,
				config.Timespan.From,
				config.Timespan.To,
				config.ParamIds,
				config.SkipParamIds,
				config.Sensors,
				config.SkipSensors,
				config.Levels,
				config.SkipLevels,
			)
			if err != nil {
				log.Error().Err(err).Msg("")
				return
			}

			labels := make([]*kvalobs.Label, 0, rows.CommandTag().RowsAffected())
			labels, err = pgx.AppendRows(labels, rows, func(row pgx.CollectableRow) (*kvalobs.Label, error) {
				label := kvalobs.Label{StationID: s.Stationid, TypeID: s.Typeid}
				err := row.Scan(&(label.ParamID), &(label.Sensor), &(label.Level))
				return &label, err
			})

			if err != nil {
				log.Error().Err(err).Msg("")
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
	bar := utils.NewBar(len(db.UniqueStationTypes), "Dumping text labels...", config.Test)

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
                        AND ($4::timestamp IS NULL OR obstime < $4)
						AND ($5::int[] IS NULL OR paramid = ANY($5))
						AND ($6::int[] IS NULL OR NOT paramid = ANY($6))`,
				s.Stationid,
				s.Typeid,
				config.Timespan.From,
				config.Timespan.To,
				config.ParamIds,
				config.SkipParamIds,
			)
			if err != nil {
				log.Error().Err(err).Msg("")
				return
			}

			labels := make([]*kvalobs.Label, 0, rows.CommandTag().RowsAffected())
			labels, err = pgx.AppendRows(labels, rows, func(row pgx.CollectableRow) (*kvalobs.Label, error) {
				label := kvalobs.Label{StationID: s.Stationid, TypeID: s.Typeid}
				err := row.Scan(&(label.ParamID))
				return &label, err
			})

			if err != nil {
				log.Error().Err(err).Msg("")
				return
			}
			sender <- labels
		}()
	}
	wg.Wait()
	close(sender)
}
