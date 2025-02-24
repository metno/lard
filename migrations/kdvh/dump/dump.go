package dump

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"migrate/utils"
)

// List of columns that we do not need to select when extracting the element codes from a KDVH table
var INVALID_COLUMNS = []string{"dato", "stnr", "typeid", "season", "xxx"}

func (table *Table) Dump(pool *pgxpool.Pool, config *Config) {
	log.Info().Str("table", table.TableName).Msg("dump started")

	fmt.Printf("Dumping %s...\n", table.TableName)
	defer fmt.Println(strings.Repeat("- ", 40))

	if err := os.MkdirAll(filepath.Join(config.Path, table.TableName), os.ModePerm); err != nil {
		log.Error().Err(err).Msg("")
		return
	}

	elements, err := table.getElements(pool, config)
	if err != nil {
		return
	}

	stations, err := table.getStations(pool, config)
	if err != nil {
		return
	}

	// Used to limit connections to the database
	semaphore := make(chan struct{}, config.MaxConn)

	for _, station := range stations {
		if !config.ShouldProcessStation(station) {
			continue
		}

		path := filepath.Join(config.Path, table.TableName, station)
		if _, err := os.Stat(path); err == nil && !config.Overwrite {
			log.Warn().Msg(fmt.Sprintf("Skipping: directory %q already exists", path))
			continue
		}

		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			log.Error().Err(err).Msg("")
			return
		}

		bar := utils.NewBar(len(elements), fmt.Sprintf("%10s", station))
		bar.RenderBlank()

		var wg sync.WaitGroup
		for _, element := range elements {
			if !config.ShouldProcessElement(element) {
				continue
			}

			wg.Add(1)

			// This blocks if the channel is full
			semaphore <- struct{}{}
			go func() {
				defer func() {
					bar.Add(1)
					wg.Done()

					// release semaphore
					<-semaphore
				}()

				err := table.DumpFn(path, element, station, pool)
				if err == nil {
					log.Info().
						Str("table_name", table.TableName).
						Str("station", station).
						Str("element", element).
						Msg("dumped successfully")
				}

			}()
		}
		wg.Wait()
	}

	log.Info().Str("table", table.TableName).Msg("dump finished")
}

// Fetch column names for a given table and filters them based on user input
// We skip the columns defined in INVALID_COLUMNS and all columns that contain the 'kopi' string
// TODO: should we dump these invalid/kopi elements even if we are not importing them?
// TODO: load from file if present?
func (table *Table) getElements(pool *pgxpool.Pool, config *Config) (elements []string, err error) {
	log.Info().Msg(fmt.Sprintf("Fetching elements for %s...", table.TableName))

	filename := filepath.Join(config.Path, table.TableName, "elements.txt")
	if fh, err := os.Open(filename); err != nil && !config.Overwrite {
		defer fh.Close()
		return utils.LoadFromFile(fh)
	}

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
		log.Error().Err(err).Msg("Could not fetch elements for table " + table.TableName)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			log.Error().Err(err).Msg("Could not fetch elements for table " + table.TableName)
			return nil, err
		}
		elements = append(elements, name)
	}

	if err := utils.SaveToFile(elements, filename); err != nil {
		log.Warn().Err(err).Msg("")
		return nil, err
	}

	return elements, nil
}

// Fetches station numbers from the elem tables and filters them based on user input
// TODO: load from file if present?
func (table *Table) getStations(pool *pgxpool.Pool, config *Config) (stations []string, err error) {
	log.Info().Msg("Fetching station numbers...")

	filename := filepath.Join(config.Path, table.TableName, "stations.txt")
	if fh, err := os.Open(filename); err != nil && !config.Overwrite {
		defer fh.Close()
		return utils.LoadFromFile(fh)
	}

	var rows pgx.Rows
	switch table.ElemTableName {
	case "T_ELEM_OBS", "T_ELEM_HOMOGEN_MONTH":
		query := fmt.Sprintf(`SELECT DISTINCT stnr FROM %s WHERE table_name = $1`, strings.ToLower(table.ElemTableName))
		rows, err = pool.Query(context.TODO(), query, table.TableName)
	case "":
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

	if err := utils.SaveToFile(stations, filename); err != nil {
		log.Warn().Err(err).Msg("")
		return nil, err
	}

	return stations, nil
}
