package dump

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kdvh/db"
	"migrate/utils"
)

// List of columns that we do not need to select when extracting the element codes from a KDVH table
var INVALID_COLUMNS = []string{"dato", "stnr", "typeid", "season", "xxx"}

func DumpTable(table *db.Table, pool *pgxpool.Pool, config *Config) {
	fmt.Printf("Dumping %s...\n", table.TableName)
	defer fmt.Println(strings.Repeat("- ", 40))

	if err := os.MkdirAll(filepath.Join(config.Path, table.Path), os.ModePerm); err != nil {
		slog.Error(err.Error())
		return
	}

	elements, err := getElements(table, pool, config)
	if err != nil {
		return
	}

	stations, err := getStations(table, pool, config)
	if err != nil {
		return
	}

	// Used to limit connections to the database
	semaphore := make(chan struct{}, config.MaxConn)

	for _, station := range stations {
		path := filepath.Join(config.Path, table.Path, station)
		if _, err := os.Stat(path); err == nil && !config.Overwrite {
			slog.Warn(fmt.Sprintf("Skipping: directory %q already exists", path))
			continue
		}

		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			return
		}

		bar := utils.NewBar(len(elements), fmt.Sprintf("%10s", station))
		bar.RenderBlank()

		var wg sync.WaitGroup
		for _, element := range elements {
			wg.Add(1)

			// This blocks if the channel is full
			semaphore <- struct{}{}
			go func() {
				defer func() {
					bar.Add(1)
					wg.Done()
				}()

				logStr := fmt.Sprintf("%s - %s - %s: ", table.TableName, station, element)

				err := table.Dump(path, element, station, logStr, config.Overwrite, pool)
				if err == nil {
					slog.Info(logStr + "dumped successfully")
				}

				// Release semaphore
				<-semaphore
			}()
		}
		wg.Wait()
	}
}

// Fetches elements and filters them based on user input
func getElements(table *db.Table, pool *pgxpool.Pool, config *Config) ([]string, error) {
	elements, err := fetchElements(table, pool)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	filename := filepath.Join(config.Path, table.Path, "elements.txt")
	if err := utils.SaveToFile(elements, filename); err != nil {
		slog.Warn(err.Error())
	}

	elements = utils.FilterSlice(config.Elements, elements, "")
	return elements, nil
}

// Fetch column names for a given table
// We skip the columns defined in INVALID_COLUMNS and all columns that contain the 'kopi' string
// TODO: should we dump these invalid/kopi elements even if we are not importing them?
func fetchElements(table *db.Table, pool *pgxpool.Pool) (elements []string, err error) {
	slog.Info(fmt.Sprintf("Fetching elements for %s...", table.TableName))

	// NOTE: T_HOMOGEN_MONTH is a special case, refer to `dumpHomogenMonth` in
	// `dump_functions.go` for more information
	if table.TableName == "T_HOMOGEN_MONTH" {
		return []string{"rr", "tam"}, nil
	}

	rows, err := pool.Query(
		context.TODO(),
		`SELECT column_name FROM information_schema.columns
            WHERE table_name = $1
            AND NOT column_name = ANY($2::text[])
            AND column_name NOT LIKE '%kopi%'`,
		// NOTE: needs to be lowercase with PG
		strings.ToLower(table.TableName),
		INVALID_COLUMNS,
	)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not fetch elements for table %s: %v", table.TableName, err))
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			slog.Error(fmt.Sprintf("Could not fetch elements for table %s: %v", table.TableName, err))
			return nil, err
		}
		elements = append(elements, name)
	}
	return elements, rows.Err()
}

// Fetches station numbers and filters them based on user input
func getStations(table *db.Table, pool *pgxpool.Pool, config *Config) ([]string, error) {
	stations, err := fetchStnrFromElemTable(table, pool)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	filename := filepath.Join(config.Path, table.Path, "stations.txt")
	if err := utils.SaveToFile(stations, filename); err != nil {
		slog.Warn(err.Error())
	}

	stations = utils.FilterSlice(config.Stations, stations, "")
	return stations, nil
}

// This function uses the ELEM table to fetch the station numbers
func fetchStnrFromElemTable(table *db.Table, pool *pgxpool.Pool) (stations []string, err error) {
	slog.Info(fmt.Sprint("Fetching station numbers..."))

	var rows pgx.Rows
	switch table.ElemTableName {
	case "T_ELEM_OBS", "T_ELEM_HOMOGEN_MONTH":
		query := fmt.Sprintf(`SELECT DISTINCT stnr FROM %s WHERE table_name = $1`, strings.ToLower(table.ElemTableName))
		rows, err = pool.Query(context.TODO(), query, table.TableName)
	case "":
		// TODO: this should be avoided if possible (only applies to T_METARDATA)
		query := fmt.Sprintf("SELECT DISTINCT stnr FROM %s", strings.ToLower(table.TableName))
		rows, err = pool.Query(context.TODO(), query)
	default:
		query := fmt.Sprintf("SELECT DISTINCT stnr FROM %s", strings.ToLower(table.ElemTableName))
		rows, err = pool.Query(context.TODO(), query)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var stnr string
		if err := rows.Scan(&stnr); err != nil {
			return nil, err
		}
		stations = append(stations, stnr)
	}

	return stations, rows.Err()
}
