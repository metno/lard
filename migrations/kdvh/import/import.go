package port

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	kdvh "migrate/kdvh/db"
	"migrate/kdvh/import/cache"
	"migrate/lard"
	"migrate/utils"
)

// TODO: add CALL_SIGN? It's not in stinfosys?
var INVALID_ELEMENTS = []string{"TYPEID", "TAM_NORMAL_9120", "RRA_NORMAL_9120", "OT", "OTN", "OTX", "DD06", "DD12", "DD18"}

func ImportTable(table *kdvh.Table, cache *cache.Cache, pool *pgxpool.Pool, config *Config) (rowsInserted int64) {
	slog.Info("table import started")
	defer fmt.Println(strings.Repeat("- ", 40))

	stations, err := os.ReadDir(filepath.Join(config.Path, table.Path))
	if err != nil {
		slog.Warn(err.Error())
		return 0
	}

	// Used to limit number of spawned threads
	// Too many threads can lead to an OOM kill, due to slice allocations in parseData
	semaphore := make(chan struct{}, config.MaxWorkers)

	// we exclude the `elements.txt` and `stations.txt` files
	bar := utils.NewBar(len(stations)-2, fmt.Sprintf("%20s", table.TableName))
	bar.RenderBlank()
	for _, station := range stations {
		stnr, err := getStationNumber(station, config.Stations)
		if err != nil {
			if config.Verbose {
				slog.Info(err.Error())
			}
			continue
		}

		stationDir := filepath.Join(config.Path, table.Path, station.Name())
		elements, err := os.ReadDir(stationDir)
		if err != nil {
			slog.Warn(err.Error())
			continue
		}

		var wg sync.WaitGroup
		for _, element := range elements {
			// This blocks if the channel is full
			semaphore <- struct{}{}

			wg.Add(1)
			go func() {
				defer func() {
					wg.Done()
					// release semaphore
					<-semaphore
				}()

				elemCode, err := getElementCode(element, config.Elements)
				if err != nil {
					if config.Verbose {
						slog.Info(err.Error())
					}
					return
				}

				tsInfo, err := cache.NewTsInfo(table.TableName, elemCode, stnr, pool)
				if err != nil {
					return
				}

				filename := filepath.Join(stationDir, element.Name())
				data, text, flag, err := parseData(filename, tsInfo, table, config)
				if err != nil {
					return
				}

				var count int64
				if tsInfo.Param.IsScalar {
					count, err = lard.InsertData(data, pool, tsInfo.Logstr)
					if err != nil {
						slog.Error(tsInfo.Logstr + "failed data bulk insertion - " + err.Error())
						return
					}
					if err := lard.InsertFlags(flag, pool, tsInfo.Logstr); err != nil {
						slog.Error(tsInfo.Logstr + "failed flag bulk insertion - " + err.Error())
					}
				} else {
					count, err = lard.InsertTextData(text, pool, tsInfo.Logstr)
					if err != nil {
						slog.Error(tsInfo.Logstr + "failed non-scalar data bulk insertion - " + err.Error())
						return
					}
				}

				rowsInserted += count
			}()
		}
		wg.Wait()
		bar.Add(1)
	}

	outputStr := fmt.Sprintf("%v: %v total rows inserted", table.TableName, rowsInserted)
	slog.Info(outputStr)
	fmt.Println(outputStr)

	return rowsInserted
}

func getStationNumber(station os.DirEntry, stationList []string) (int32, error) {
	if !station.IsDir() {
		return 0, errors.New(fmt.Sprintf("%s is not a directory, skipping", station.Name()))
	}

	if len(stationList) > 0 && !slices.Contains(stationList, station.Name()) {
		return 0, errors.New(fmt.Sprintf("Station %v not in the list, skipping", station.Name()))
	}

	stnr, err := strconv.ParseInt(station.Name(), 10, 32)
	if err != nil {
		return 0, errors.New("Error parsing station number:" + err.Error())
	}

	return int32(stnr), nil
}

func elemcodeIsInvalid(element string) bool {
	return strings.Contains(element, "KOPI") || slices.Contains(INVALID_ELEMENTS, element)
}

func getElementCode(element os.DirEntry, elementList []string) (string, error) {
	elemCode := strings.ToUpper(strings.TrimSuffix(element.Name(), ".csv"))

	if len(elementList) > 0 && !slices.Contains(elementList, elemCode) {
		return "", errors.New(fmt.Sprintf("Element %q not in the list, skipping", elemCode))
	}

	if elemcodeIsInvalid(elemCode) {
		return "", errors.New(fmt.Sprintf("Element %q not set for import, skipping", elemCode))
	}
	return elemCode, nil
}

// Parses the observations in the CSV file, converts them with the table
// ConvertFunction and returns three arrays that can be passed to pgx.CopyFromRows
func parseData(filename string, tsInfo *kdvh.TsInfo, table *kdvh.Table, config *Config) ([][]any, [][]any, [][]any, error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Warn(err.Error())
		return nil, nil, nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var rowCount int
	// Try to infer row count from header
	if !config.NoHeader {
		scanner.Scan()
		rowCount, _ = strconv.Atoi(scanner.Text())
	}

	data := make([][]any, 0, rowCount)
	text := make([][]any, 0, rowCount)
	flag := make([][]any, 0, rowCount)

	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), config.Sep)

		obsTime, err := time.Parse("2006-01-02_15:04:05", cols[0])
		if err != nil {
			return nil, nil, nil, err
		}

		if table.MaxImportYearReached(obsTime.Year()) {
			break
		}

		// Only import data between KDVH's defined fromtime and totime
		if tsInfo.Timespan.From != nil && obsTime.Sub(*tsInfo.Timespan.From) < 0 {
			continue
		} else if tsInfo.Timespan.To != nil && obsTime.Sub(*tsInfo.Timespan.To) > 0 {
			break
		}

		obs := kdvh.KdvhObs{Obstime: obsTime, Data: cols[1], Flags: cols[2]}
		dataRow, textRow, flagRow, err := table.Convert(&obs, tsInfo)
		if err != nil {
			return nil, nil, nil, err
		}
		data = append(data, dataRow.ToRow())
		text = append(text, textRow.ToRow())
		flag = append(flag, flagRow.ToRow())
	}

	if len(data) == 0 {
		if config.Verbose {
			slog.Info(tsInfo.Logstr + "no rows to insert (all obstimes > max import time)")
		}
		return nil, nil, nil, errors.New("No rows to insert")
	}

	return data, text, flag, nil
}
