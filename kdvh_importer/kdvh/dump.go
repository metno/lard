package kdvh

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"kdvh_importer/dump"
	"kdvh_importer/utils"
)

func (table *KDVHTable) Dump(conn *sql.DB, config *dump.Config) {
	defer utils.SendEmailOnPanic(fmt.Sprintf("%s dump", table.TableName), config.Email)

	// TODO: should probably do it at the station/element level?
	// TODO: remove the '_combined'
	outdir := filepath.Join(config.BaseDir, table.TableName+"_combined")
	if _, err := os.ReadDir(outdir); err == nil && !config.Overwrite {
		slog.Info(fmt.Sprint("Skipping data dump of ", table.TableName, " because dumped folder already exists"))
		return
	}

	slog.Info(fmt.Sprint("Starting dump of ", table.TableName))
	utils.SetLogFile(table.TableName, "dump")

	elements, err := getElements(table, conn, config)
	if err != nil {
		return
	}

	// TODO: should be safe to spawn goroutines/waitgroup here with connection pool?
	for _, element := range elements {
		stations, err := getStationsWithElement(element, table, conn, config)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not fetch stations for table %s: %v", table.TableName, err))
			continue
		}

		for _, station := range stations {
			path := filepath.Join(outdir, string(station))
			if err := os.MkdirAll(path, os.ModePerm); err != nil {
				slog.Error(err.Error())
				continue
			}

			err := table.DumpFunc(
				dumpFuncArgs{
					path:      path,
					element:   element,
					station:   station,
					dataTable: table.TableName,
					flagTable: table.FlagTableName,
				},
				conn,
			)

			// NOTE: Non-nil errors are logged inside each DumpFunc
			if err == nil {
				slog.Info(fmt.Sprintf("%s - %s - %s: dumped successfully", table.TableName, station, element))
			}
		}
	}

	log.SetOutput(os.Stdout)
	log.Println("Finished dump of", table.TableName)
}

func getElements(table *KDVHTable, conn *sql.DB, config *dump.Config) ([]string, error) {
	elements, err := table.fetchElements(conn)
	if err != nil {
		return nil, err
	}

	elements = utils.FilterSlice(config.Elements, elements, "")
	return elements, nil
}

func getStationsWithElement(element string, table *KDVHTable, conn *sql.DB, config *dump.Config) ([]string, error) {
	stations, err := table.fetchStationsWithElement(element, conn)
	if err != nil {
		return nil, err
	}

	msg := fmt.Sprintf("Element '%s'", element) + "not available for station '%s'"
	stations = utils.FilterSlice(config.Stations, stations, msg)
	return stations, nil
}

func (table *KDVHTable) fetchElements(conn *sql.DB) ([]string, error) {
	// TODO: not sure why we only dump these two for this table
	// TODO: separate this to its own function? Separate edge cases
	if table.TableName == "T_HOMOGEN_MONTH" {
		return []string{"rr", "tam"}, nil
	}

	elements, err := fetchColumnNames(table.TableName, conn)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not fetch elements for table %s: %v", table.TableName, err))
		return nil, err
	}

	return elements, nil
}

// List of columns that we do not need to select when extracting the element codes from a KDVH table
// TODO: what's the difference between obs_origtime and klobs (they have same paramid)?
// Should they be added here? Do we need to exclude other elements?
var INVALID_COLUMNS = []string{"dato", "stnr", "typeid", "season", "xxx"}

// Fetch column names for a given table
// We skip the columns defined in INVALID_COLUMNS and all columns that contain the 'kopi' string
func fetchColumnNames(tableName string, conn *sql.DB) ([]string, error) {
	slog.Info(fmt.Sprintf("Fetching elements for %s...", tableName))

	rows, err := conn.Query(
		`SELECT column_name FROM information_schema.columns
            WHERE table_name = $1
            AND NOT column_name = ANY($2::text[])
            AND column_name NOT LIKE '%kopi%'`,
		// NOTE: needs to be lowercase with PG
		strings.ToLower(tableName),
		INVALID_COLUMNS,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var elements []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		elements = append(elements, name)
	}
	return elements, rows.Err()
}

func fetchStationNumbers(table *KDVHTable, conn *sql.DB) ([]string, error) {
	slog.Info(fmt.Sprint("Fetching station numbers (this can take a while)..."))

	// FIXME:? this query can be extremely slow
	query := fmt.Sprintf(
		`SELECT DISTINCT stnr FROM %s`,
		table.TableName,
	)

	// TODO: probably unnecessary
	if table.FlagTableName != "" {
		query = fmt.Sprintf(
			`(SELECT stnr FROM %s) UNION (SELECT stnr FROM %s)`,
			table.TableName,
			table.FlagTableName,
		)
	}

	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stations := make([]string, 0)
	for rows.Next() {
		var stnr string
		if err := rows.Scan(&stnr); err != nil {
			return nil, err
		}
		stations = append(stations, stnr)
	}

	return stations, rows.Err()
}

// NOTE: inverting the loops and splitting by element does make it a bit better,
// because we avoid quering for tables that have no data or flag for that element
func (table *KDVHTable) fetchStationsWithElement(element string, conn *sql.DB) ([]string, error) {
	slog.Info(fmt.Sprintf("Fetching station numbers for %s (this can take a while)...", element))

	query := fmt.Sprintf(
		`SELECT DISTINCT stnr FROM %s WHERE %s IS NOT NULL`,
		table.TableName,
		element,
	)

	rows, err := conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stations := make([]string, 0)
	for rows.Next() {
		var stnr string
		if err := rows.Scan(&stnr); err != nil {
			return nil, err
		}
		stations = append(stations, stnr)
	}

	return stations, rows.Err()
}
