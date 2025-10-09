package port

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
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
func (table *Table) Import(path string, cache *Cache, pools *lard.Pools, config *Config) (int64, error) {
	if !config.Test {
		handle := utils.SetLoggerOutput(strings.ReplaceAll(path, "/", "_"), "import")
		defer handle.Close()
	}

	log.Info().Str("span", path).Msg("import started")

	stations, err := os.ReadDir(path)
	if err != nil {
		log.Error().Err(err).Msg("")
		return 0, err
	}

	// Used to limit number of spawned threads
	// Too many threads can lead to an OOM kill, due to slice allocations in table.Import
	semaphore := make(chan struct{}, config.MaxWorkers)

	bar := utils.NewBar(len(stations), fmt.Sprintf("Importing %s stations...", table.Name), config.Test)
	bar.RenderBlank()

	var rowsInserted int64
	for _, station := range stations {
		stnr, err := utils.Atoi32(station.Name())
		if err != nil || !config.ShouldProcessStation(stnr) {
			bar.Add(1)
			continue
		}

		stationDir := filepath.Join(path, station.Name())
		files, err := os.ReadDir(stationDir)
		if err != nil {
			log.Warn().Err(err).Msg("")
			bar.Add(1)
			continue
		}

		var wg sync.WaitGroup
		for _, file := range files {
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

				tsid, pool, err := table.getTsidAndDbPool(label, cache, pools)
				if err != nil {
					log.Error().Err(err).Interface("label", label).Msg("")
					return
				}

				if (config.SkipRestricted && pool == pools.Restricted) || (config.SkipOpen && pool == pools.Open) {
					return
				}

				filename := filepath.Join(stationDir, file.Name())
				parsed, err := parseDump(filename, tsid, label, table)
				if err != nil {
					log.Error().Err(err).Interface("label", label).Msg("")
					return
				}

				err = parsed.UpdateFromtime(pool)
				if err != nil {
					log.Error().Err(err).Msg("")
					return
				}

				count, err := parsed.Insert(pool)
				if err != nil {
					log.Error().Err(err).Msg("")
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

func parseDump(filename string, tsid int64, label *kvalobs.Label, table *Table) (*lard.ParsedCsv, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bufreader := bufio.NewReader(file)

	firstRow, err := bufreader.ReadSlice('\n')
	if err != nil {
		return nil, err
	}

	rowCount, _ := strconv.Atoi(string(firstRow))

	headers, err := bufreader.ReadSlice('\n')
	if err != nil {
		return nil, err
	}

	seekLen := len(firstRow) + len(headers)
	_, err = file.Seek(int64(seekLen), io.SeekStart)
	if err != nil {
		return nil, err
	}

	csvReader := csv.NewReader(file)

	if label.IsMetarCloudType() {
		parsed, err := parseMetarCloudType(tsid, rowCount, csvReader)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	}

	if label.IsSpecialCloudType() {
		parsed, err := parseSpecialCloudType(tsid, rowCount, csvReader)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	}

	switch table.Name {
	case kvalobs.DataTableName:
		parsed, err := parseData(tsid, rowCount, csvReader)
		if err != nil {
			return nil, err
		}
		return parsed, nil

	case kvalobs.TextTableName:
		parsed, err := parseText(tsid, rowCount, csvReader)
		if err != nil {
			return nil, err
		}
		return parsed, nil
	}

	return nil, nil
}

func (table *Table) getTsidAndDbPool(label *kvalobs.Label, cache *Cache, pools *lard.Pools) (int64, *pgxpool.Pool, error) {
	innerPool := pools.Restricted

	permit := cache.GetPermit(label.StationID, label.TypeID, label.ParamID)
	if permit != nil && *permit == 1 {
		innerPool = pools.Open
	}

	// convert to 0 if pointer is nil
	var lvl = int32(0)
	if label.Level != nil {
		lvl = *label.Level
	}
	level := cache.Levels.GetLevel(label.ParamID, lvl)

	lardLabel := lard.Label{
		StationID: label.StationID,
		TypeID:    label.TypeID,
		ParamID:   label.ParamID,
		Sensor:    label.Sensor,
		LegacyLvl: label.Level,
		Level:     level,
	}

	tsid, err := lardLabel.CreateKvalobsTimeseries(permit, innerPool)
	if err != nil {
		return 0, nil, err
	}

	return tsid, innerPool, nil
}
