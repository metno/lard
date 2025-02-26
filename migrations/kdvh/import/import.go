package port

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	kdvh "migrate/kdvh/db"
	"migrate/lard"
	"migrate/utils"
)

const TIME_FORMAT string = "2006-01-02_15:04:05"

// TODO: add CALL_SIGN? It's not in stinfosys?
var INVALID_ELEMENTS = []string{"TYPEID", "TAM_NORMAL_9120", "RRA_NORMAL_9120", "OT", "OTN", "OTX", "DD06", "DD12", "DD18"}

func (table *Table) Import(cache *Cache, pool *pgxpool.Pool, config *Config) (rowsInserted int64) {
	handle := utils.SetLoggerOutput(table.TableName, "import")
	defer handle.Close()

	log.Info().Str("table", table.TableName).Msg("import started")
	defer fmt.Println(strings.Repeat("- ", 40))

	tableDir := filepath.Join(config.Path, table.TableName)
	stations, err := os.ReadDir(tableDir)
	if err != nil {
		log.Error().Err(err).Msg("")
		return 0
	}

	// Used to limit number of spawned threads
	// Too many threads can lead to an OOM kill, due to slice allocations in parseData
	semaphore := make(chan struct{}, config.MaxWorkers)

	// we exclude the `elements.txt` and `stations.txt` files
	bar := utils.NewBar(len(stations)-2, fmt.Sprintf("%20s", table.TableName))
	bar.RenderBlank()
	for _, station := range stations {
		if !station.IsDir() || !config.ShouldProcessStation(station.Name()) {
			continue
		}

		stnr, err := utils.Atoi32(station.Name())
		if err != nil {
			log.Error().Err(err).Msg("")
			continue
		}

		stationDir := filepath.Join(tableDir, station.Name())
		elements, err := os.ReadDir(stationDir)
		if err != nil {
			log.Error().Err(err).Msg("")
			continue
		}

		var wg sync.WaitGroup
		for _, element := range elements {
			elemCode := strings.ToUpper(strings.TrimSuffix(element.Name(), ".csv"))
			if !config.ShouldProcessElement(elemCode) || elemcodeIsInvalid(elemCode) {
				continue
			}

			// This blocks if the channel is full
			semaphore <- struct{}{}

			wg.Add(1)
			go func() {
				defer func() {
					// release semaphore
					<-semaphore
					wg.Done()
				}()

				logger := log.Logger.With().
					Str("table", table.TableName).
					Int32("station", stnr).
					Str("element", elemCode).Logger()

				tsInfo, err := cache.NewTsInfo(table.TableName, elemCode, stnr, pool)
				if err != nil {
					logger.Error().Err(err).Msg("")
					return
				}

				filename := filepath.Join(stationDir, element.Name())
				parsed, err := parseData(filename, tsInfo, table, config)
				if err != nil {
					logger.Error().Err(err).Msg("")
					return
				}

				count, err := parsed.Insert(pool)
				if err != nil {
					logger.Error().Err(err).Msg("")
					return
				}

				if count > 0 {
					logger.Info().Int64("n_rows", count).Msg("")
					rowsInserted += count
				} else {
					logger.Warn().Msg("No data to insert")
				}
			}()
		}
		wg.Wait()
		bar.Add(1)
	}

	log.Info().Str("table", table.TableName).Int64("total_rows", rowsInserted).Msg("import finished")
	fmt.Printf("%v: %v total rows inserted\n", table.TableName, rowsInserted)

	return rowsInserted
}

func elemcodeIsInvalid(element string) bool {
	return strings.Contains(element, "KOPI") || slices.Contains(INVALID_ELEMENTS, element)
}

// Parses the observations in the CSV file, converts them with the table
// ConvertFunction and returns three arrays that can be passed to pgx.CopyFromRows
func parseData(filename string, tsInfo *kdvh.TsInfo, table *Table, config *Config) (*lard.ParsedCsv, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var rowCount int
	// Try to infer row count from header
	if !config.NoHeader {
		scanner.Scan()
		rowCount, _ = strconv.Atoi(scanner.Text())
	}

	parsed := lard.InitParsedCsv(rowCount)
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), config.Sep)

		obsTime, err := time.Parse(TIME_FORMAT, cols[0])
		if err != nil {
			return nil, err
		}

		if obsTime.Year() >= table.ImportUntil {
			break
		}

		// Only import data between KDVH's defined fromtime and totime
		if tsInfo.Timespan.From != nil && obsTime.Sub(*tsInfo.Timespan.From) < 0 {
			continue
		}

		if tsInfo.Timespan.To != nil && obsTime.Sub(*tsInfo.Timespan.To) > 0 {
			break
		}

		obs := kdvh.Obs{Obstime: obsTime, Data: cols[1], Flags: cols[2]}
		converted, err := table.Convert(&obs, tsInfo)
		if err != nil {
			return nil, err
		}

		parsed.Append(converted)
	}

	return parsed, nil
}
