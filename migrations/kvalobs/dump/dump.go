package dump

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"migrate/utils"
)

func (table *Table) dump(stations StationMap, path string, pool *pgxpool.Pool, config *Config) {
	log.Info().Str("span", path).Msg("dump started")
	fmt.Printf("Dumping to %q...\n", path)
	defer fmt.Println(strings.Repeat("- ", 40))

	// Used to limit connections to the database
	semaphore := make(chan struct{}, config.MaxConn)
	var wg sync.WaitGroup

	bar := utils.NewBar(len(stations), "Dumping stations...", config.Test)
	bar.RenderBlank()

	for pair, labels := range stations {
		stationPath := filepath.Join(path, fmt.Sprint(pair.Stationid))

		for _, label := range labels {
			wg.Add(1)
			semaphore <- struct{}{}

			go func() {
				if err := table.DumpSeries(label, &config.Timespan, stationPath, pool); err == nil {
					log.Info().Interface("label", label).Msg("dumped successfully")
				}
				<-semaphore
				wg.Done()
			}()
		}
		wg.Wait()
		bar.Add(1)
	}

	log.Info().Str("span", path).Msg("dump finished")
}

func (database *Database) dump(spanPath string, config *Config) {
	pool, err := pgxpool.New(context.Background(), os.Getenv(database.ConnEnvVar))
	if err != nil {
		log.Error().Err(err).Msg("Could not connect to Kvalobs")
		return
	}
	defer pool.Close()

	for _, table := range database.Tables {
		if !utils.StringIsEmptyOrEqual(config.Table, table.Name) {
			continue
		}

		// <db_name>_<table_name>_<timespan>_<utc_now>_dump.log
		logFile := strings.Join([]string{database.Name, table.Name, spanPath}, "_")
		handle := utils.SetLoggerOutput(logFile, "dump")
		defer handle.Close()

		path := filepath.Join(
			config.Path,
			database.Name,
			spanPath,
			table.Name,
		)
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			log.Error().Err(err).Msg("")
			return
		}

		stations, err := getStationLabelMap(table, database, path, pool, config)
		if err != nil {
			return
		}

		table.dump(stations, path, pool, config)
	}
}
