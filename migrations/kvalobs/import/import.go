package port

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	kvalobs "migrate/kvalobs/db"
	"migrate/lard"
	"migrate/utils"
)

var RESTRICTED_TS_ERROR = fmt.Errorf("Restricted data")

// NOTE: we return the number of inserted rows for the tests
func (table *Table) Import(cache *Cache, pool *pgxpool.Pool, config *Config) (int64, error) {
	tag := fmt.Sprintf("%s_%s_%s", table.DbName, table.Name, config.SpanDir)
	handle := utils.SetLoggerOutput(tag, "import")
	defer handle.Close()

	path := filepath.Join(config.Path, table.DbName, table.Name, config.SpanDir)
	log.Info().Str("span", path).Msg("import started")

	fmt.Printf("Importing from %q...\n", path)
	defer fmt.Println(strings.Repeat("- ", 40))

	stations, err := os.ReadDir(path)
	if err != nil {
		log.Error().Err(err).Msg("")
		return 0, err
	}

	importSpan, err := utils.TimespanFromDirName(config.SpanDir)
	if err != nil {
		log.Error().Err(err).Msg("")
		return 0, err
	}

	// Used to limit number of spawned threads
	// Too many threads can lead to an OOM kill, due to slice allocations in table.Import
	semaphore := make(chan struct{}, config.MaxWorkers)

	bar := utils.NewBar(len(stations), fmt.Sprintf("Importing %s stations...", table.Name))
	bar.RenderBlank()

	var rowsInserted int64
	for _, station := range stations {
		stnr, err := utils.Atoi32(station.Name())
		if err != nil || !config.ShouldProcessStation(stnr) {
			bar.Add(1)
			continue
		}

		stationDir := filepath.Join(path, station.Name())
		labels, err := os.ReadDir(stationDir)
		if err != nil {
			log.Warn().Err(err).Msg("")
			bar.Add(1)
			continue
		}

		var wg sync.WaitGroup
		for _, file := range labels {
			semaphore <- struct{}{}
			wg.Add(1)

			go func() {
				defer func() {
					<-semaphore
					wg.Done()
				}()

				label, err := kvalobs.LabelFromFilename(file.Name())
				if err != nil {
					log.Error().Err(err).Msg("")
					return
				}

				if !config.ShouldProcessLabel(label) {
					return
				}

				tsid, err := table.getTsid(label, *importSpan, cache, pool)
				if err != nil {
					if errors.Is(err, RESTRICTED_TS_ERROR) {
						log.Warn().Interface("label", label).Msg("timeseries data is restricted, skipping")
					} else {
						log.Error().Err(err).Interface("label", label).Msg("")
					}
					return
				}

				filename := filepath.Join(stationDir, file.Name())
				file, err := os.Open(filename)
				if err != nil {
					log.Error().Err(err).Interface("label", label).Msg("")
					return
				}
				defer file.Close()

				parser := table.getParser(label)
				count, err := importLabel(file, tsid, label, pool, parser)
				if err != nil {
					log.Error().Err(err).Interface("label", label).Msg("")
					return
				}

				log.Info().Interface("label", label).Int64("n_rows", count).Msg("")
				rowsInserted += count
			}()
		}
		wg.Wait()
		bar.Add(1)
	}

	log.Info().Str("span", path).Int64("total_rows", rowsInserted).Msg("import finished")
	fmt.Printf("%v: %v total rows inserted\n", path, rowsInserted)

	return rowsInserted, nil
}

func importLabel(file *os.File, tsid int64, label *kvalobs.Label, pool *pgxpool.Pool, parser ParseFunc) (count int64, err error) {
	scanner := bufio.NewScanner(file)

	// Parse number of rows
	scanner.Scan()
	rowCount, _ := strconv.Atoi(scanner.Text())

	// Skip header
	scanner.Scan()

	parsed := lard.NewParsedCsv(rowCount)
	for scanner.Scan() {
		obs, err := parser(tsid, scanner.Text())
		if err != nil {
			log.Error().Err(err).Interface("label", label).Msg("")
			return 0, err
		}
		parsed.Append(obs)
	}

	return parsed.Insert(pool)
}

func (table *Table) getTsid(label *kvalobs.Label, importSpan utils.TimeSpan, cache *Cache, pool *pgxpool.Pool) (int64, error) {
	// Check if data for this station/element is restricted
	if !cache.TimeseriesIsOpen(label.StationID, label.TypeID, label.ParamID) {
		// TODO: eventually use this to choose which table to use on insert
		return 0, RESTRICTED_TS_ERROR
	}

	// TODO: this can never error right now?
	tsTimespan, err := cache.GetSeriesTimespan(table.DbName, label)
	if err != nil {
		return 0, err
	}

	// TODO: figure out where to get fromtime, kvalobs directly? Stinfosys?
	lardLabel := label.ToLard()
	tsid, err := lardLabel.CreateKvalobsTimeseries(importSpan, tsTimespan, pool)
	if err != nil {
		return 0, err
	}

	return tsid, nil
}

func (table *Table) ImportAllTimespans(cache *Cache, pool *pgxpool.Pool, config *Config) (int64, error) {
	path := filepath.Join(config.Path, table.DbName, table.Name)
	timespans, err := os.ReadDir(path)
	if err != nil {
		log.Error().Err(err).Msg("")
		return 0, err
	}

	for _, span := range timespans {
		if !span.IsDir() {
			continue
		}
		// HACK: modify spandir in place
		config.SpanDir = span.Name()
		table.Import(cache, pool, config)
	}

	return 0, nil
}
