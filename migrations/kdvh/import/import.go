package port

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	kdvh "migrate/kdvh/db"
	"migrate/lard"
	"migrate/utils"
)

const TIME_FORMAT string = "2006-01-02_15:04:05"

// TODO: add CALL_SIGN? It's not in stinfosys?
var INVALID_ELEMENTS = []string{"TYPEID", "TAM_NORMAL_9120", "RRA_NORMAL_9120", "OT", "OTN", "OTX", "DD06", "DD12", "DD18"}

func (table *Table) Import(tableDir string, cache *Cache, pools *lard.Pools, config *Config) (rowsInserted int64) {
	if !config.Test {
		handle := utils.SetLoggerOutput(table.Name, "import")
		defer handle.Close()
	}

	stations, err := os.ReadDir(tableDir)
	if err != nil {
		// tableDir does not exist if the table was not dumped
		return 0
	}

	log.Info().Str("table", table.Name).Msg("import started")

	// Used to limit number of spawned threads
	// Too many threads can lead to an OOM kill, due to slice allocations in parseData
	semaphore := make(chan struct{}, config.MaxWorkers)

	bar := utils.NewBar(len(stations), fmt.Sprintf("%20s", table.Name), config.Test)
	bar.RenderBlank()

	for _, station := range stations {
		if !station.IsDir() || !config.ShouldProcessStation(station.Name()) {
			bar.Add(1)
			continue
		}

		stnr, err := utils.Atoi32(station.Name())
		if err != nil {
			log.Error().Err(err).Msg("")
			bar.Add(1)
			continue
		}

		stationDir := filepath.Join(tableDir, station.Name())
		elements, err := os.ReadDir(stationDir)
		if err != nil {
			log.Error().Err(err).Msg("")
			bar.Add(1)
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
					Str("table", table.Name).
					Int32("station", stnr).
					Str("element", elemCode).Logger()

				filename := filepath.Join(stationDir, element.Name())
				file, err := ShouldImport(filename, table, config)
				if err != nil {
					logger.Error().Err(err).Msg("")
					return
				}
				defer file.Close()

				tsInfo, pool, err := GetTsInfoAndDbPool(table.Name, elemCode, stnr, cache, pools)
				if err != nil {
					logger.Error().Err(err).Msg("")
					return
				}

				if (config.SkipRestricted && pool == pools.Restricted) || (config.SkipOpen && pool == pools.Open) {
					return
				}

				if (config.SkipScalar && tsInfo.IsScalar) || (config.SkipText && !tsInfo.IsScalar) {
					return
				}

				parsed, err := parseData(file, tsInfo, table, config)
				if err != nil {
					logger.Error().Err(err).Msg("")
					return
				}

				err = parsed.UpdateFromtime(pool)
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

	log.Info().Str("table", table.Name).Int64("total_rows", rowsInserted).Msg("import finished")
	fmt.Printf("%v: %v total rows inserted\n", table.Name, rowsInserted)

	return rowsInserted
}

func elemcodeIsInvalid(element string) bool {
	return strings.Contains(element, "KOPI") || slices.Contains(INVALID_ELEMENTS, element)
}

// Returns the file to read if at least one of it's rows should be imported.
// Avoids creating timeseries without data in LARD.
// Closes the file if any errors occur after it has been opened.
func ShouldImport(filename string, table *Table, config *Config) (*os.File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)

	if !config.NoHeader {
		scanner.Scan()
	}

	if scanner.Scan() {
		cols := strings.Split(scanner.Text(), config.Sep)

		obsTime, err := time.Parse(TIME_FORMAT, cols[0])
		if err != nil {
			file.Close()
			return nil, err
		}

		if obsTime.Year() >= table.ImportUntil {
			file.Close()
			return nil, fmt.Errorf("No data to import")
		}
	}

	// Return to the start of the file
	_, err = file.Seek(0, 0)
	if err != nil {
		file.Close()
		return nil, err
	}

	return file, nil
}

func parseRecord(csv []string) (*kdvh.Obs, error) {
	var data, flags string

	obsTime, err := time.Parse(TIME_FORMAT, csv[0])
	if err != nil {
		return nil, err
	}

	switch len(csv) {
	// Originally the dumps did not contain typeid
	case 3:
		data = csv[1]
		flags = csv[2]
	// Now they also contain typeid, but it's not used during import
	// Sucks I decided to save it in position 1
	case 4:
		// typeid = csv[1]
		data = csv[2]
		flags = csv[3]
	default:
		return nil, fmt.Errorf("Invalid number of CSV columns")
	}

	return &kdvh.Obs{Obstime: obsTime, Data: data, Flags: flags}, nil
}

// Parses the observations in the CSV file, converts them with the table
// ConvertFunction and returns three arrays that can be passed to pgx.CopyFromRows
func parseData(file *os.File, tsInfo *kdvh.TsInfo, table *Table, config *Config) (*lard.ParsedCsv, error) {
	bufreader := bufio.NewReader(file)

	firstRow, err := bufreader.ReadSlice('\n')
	if err != nil {
		return nil, err
	}

	// Try to infer row count from header
	rowCount, _ := strconv.Atoi(string(firstRow))

	_, err = file.Seek(int64(len(firstRow)), io.SeekStart)
	if err != nil {
		return nil, err
	}

	csvReader := csv.NewReader(file)
	// HACK: sucks you can't pass runes via cli since they are just ints
	// The config is validated before arriving here
	csvReader.Comma = []rune(config.Sep)[0]

	// TODO: this might allocate more than we need, since we have a break condition in the loop
	parsed := lard.NewParsedCsv(rowCount)
	for {
		fields, err := csvReader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		obs, err := parseRecord(fields)
		if err != nil {
			return nil, err
		}

		if obs.Obstime.Year() >= table.ImportUntil {
			break
		}

		converted, err := table.Convert(obs, tsInfo)
		if err != nil {
			return nil, err
		}

		parsed.Append(converted)
	}

	return parsed, nil
}
