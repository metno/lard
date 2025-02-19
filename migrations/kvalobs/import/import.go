package port

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	kvalobs "migrate/kvalobs/db"
	"migrate/lard"
	"migrate/utils"
)

// NOTE: we return the number of inserted rows for the tests
func (table *Table) Import(cache *Cache, pool *pgxpool.Pool, config *Config) (int64, error) {
	path := filepath.Join(config.Path, table.DbName, table.Name, config.SpanDir)

	tag := fmt.Sprintf("%s_%s_%s", table.DbName, table.Name, config.SpanDir)
	handle := utils.SetLogFile(tag, "import")
	defer handle.Close()

	fmt.Printf("Importing from %q...\n", path)
	defer fmt.Println(strings.Repeat("- ", 40))

	stations, err := os.ReadDir(path)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}

	importSpan, err := utils.TimespanFromDirName(config.SpanDir)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}

	// Used to limit number of spawned threads
	// Too many threads can lead to an OOM kill, due to slice allocations in table.Import
	semaphore := make(chan struct{}, config.MaxWorkers)
	bar := utils.NewBar(len(stations), fmt.Sprintf("Importing %s stations...", table.Name))
	bar.RenderBlank()

	var rowsInserted int64
	for _, station := range stations {
		stnr, err := strconv.ParseInt(station.Name(), 10, 32)
		if err != nil || !utils.IsNilOrContains(config.Stations, int32(stnr)) {
			bar.Add(1)
			continue
		}

		stationDir := filepath.Join(path, station.Name())
		labels, err := os.ReadDir(stationDir)
		if err != nil {
			slog.Warn(err.Error())
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
					slog.Error(err.Error())
					return
				}

				if !config.ShouldProcessLabel(label) {
					return
				}

				tsid, err := table.getTsid(label, *importSpan, cache, pool)
				if err != nil {
					slog.Error(label.LogStr() + err.Error())
					return
				}

				filename := filepath.Join(stationDir, file.Name())
				file, err := os.Open(filename)
				if err != nil {
					slog.Error(label.LogStr() + err.Error())
					return
				}
				defer file.Close()

				parser := table.getParser(label)
				count, err := importLabel(file, tsid, label, pool, parser)
				if err == nil {
					rowsInserted += count
				}
			}()
		}
		wg.Wait()
		bar.Add(1)
	}

	outputStr := fmt.Sprintf("%v: %v total rows inserted", path, rowsInserted)
	slog.Info(outputStr)
	fmt.Println(outputStr)

	return rowsInserted, nil
}

func importLabel(file *os.File, tsid int64, label *kvalobs.Label, pool *pgxpool.Pool, parser ParseFunc) (count int64, err error) {
	logStr := label.LogStr()
	scanner := bufio.NewScanner(file)

	// Parse number of rows
	scanner.Scan()
	rowCount, _ := strconv.Atoi(scanner.Text())

	// Skip header
	scanner.Scan()

	parsed := lard.InitParsedCsv(rowCount)
	for scanner.Scan() {
		obs, err := parser(tsid, scanner.Text())
		if err != nil {
			slog.Error(logStr + err.Error())
			return 0, err
		}
		parsed.Append(obs)
	}

	// TODO: could also simply return
	count, err = parsed.Insert(pool, logStr)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}

	return count, nil
}

func (table *Table) getTsid(label *kvalobs.Label, importSpan utils.TimeSpan, cache *Cache, pool *pgxpool.Pool) (int64, error) {
	// Check if data for this station/element is restricted
	if !cache.TimeseriesIsOpen(label.StationID, label.TypeID, label.ParamID) {
		slog.Warn("timeseries data is restricted, skipping")
		// TODO: eventually use this to choose which table to use on insert
		return 0, fmt.Errorf("Restricted data")
	}

	// TODO: this can never error right now?
	tsTimespan, err := cache.GetSeriesTimespan(label)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}

	// TODO: figure out where to get fromtime, kvalobs directly? Stinfosys?
	tsid, err := label.ToLard().CreateKvalobsTimeseries(table.DbName, table.Name, importSpan, tsTimespan, pool)
	if err != nil {
		slog.Error(err.Error())
		return 0, err
	}

	return tsid, nil
}

func (table *Table) ImportAllTimespans(cache *Cache, pool *pgxpool.Pool, config *Config) (int64, error) {
	path := filepath.Join(config.Path, table.DbName, table.Name)
	timespans, err := os.ReadDir(path)
	if err != nil {
		slog.Error(err.Error())
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
