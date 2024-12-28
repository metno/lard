package db

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Format string for date field in CSV files
const TIMEFORMAT string = "2006-01-02_15:04:05"

// Error returned if no observations are found for a (station, element) pair
var EMPTY_QUERY_ERR error = errors.New("The query did not return any rows")

// Struct representing a single record in the output CSV file
type Record struct {
	Time time.Time      `db:"time"`
	Data sql.NullString `db:"data"`
	Flag sql.NullString `db:"flag"`
}

// Helper function for dumpByYear functinos Fetch min and max year from table, needed for tables that are dumped by year
func fetchYearRange(tableName, station, element string, pool *pgxpool.Pool) (begin int32, end int32, err error) {
	query := fmt.Sprintf(
		`SELECT min(EXTRACT(year FROM dato)), max(EXTRACT(year FROM dato)) FROM %s 
            WHERE %s IS NOT NULL
            AND stnr = $1`,
		tableName,
		element,
	)

	err = pool.QueryRow(context.TODO(), query, station).Scan(&begin, &end)
	return begin, end, err
}

// This function is used when the table contains large amount of data
// (T_SECOND, T_MINUTE, T_10MINUTE)
func dumpByYear(path string, args dumpArgs, logStr string, overwrite bool, pool *pgxpool.Pool) error {
	dataBegin, dataEnd, err := fetchYearRange(args.dataTable, args.station, args.element, pool)
	if err != nil {
		return err
	}

	flagBegin, flagEnd, err := fetchYearRange(args.flagTable, args.station, args.element, pool)
	if err != nil {
		return err
	}

	begin := min(dataBegin, flagBegin)
	end := max(dataEnd, flagEnd)

	query := fmt.Sprintf(
		`SELECT
            dato AS time,
            d.%[1]s AS data,
            f.%[1]s AS flag
        FROM
            (SELECT dato, stnr, %[1]s FROM %[2]s
                WHERE %[1]s IS NOT NULL AND stnr = $1 AND EXTRACT(year FROM dato) = $2) d
        FULL OUTER JOIN
            (SELECT dato, stnr, %[1]s FROM %[3]s
                WHERE %[1]s IS NOT NULL AND stnr = $1 AND EXTRACT(year FROM dato) = $2) f
        USING (dato)`,
		args.element,
		args.dataTable,
		args.flagTable,
	)

	for year := begin; year < end; year++ {
		yearPath := filepath.Join(path, fmt.Sprint(year))
		if err := os.MkdirAll(yearPath, os.ModePerm); err != nil {
			slog.Error(logStr + err.Error())
			return err
		}

		rows, err := pool.Query(context.TODO(), query, args.station, year)
		if err != nil {
			slog.Error(logStr + "Could not query KDVH - " + err.Error())
			return err
		}

		filename := filepath.Join(yearPath, args.element+".csv")
		if err := writeToCsv(filename, rows); err != nil {
			slog.Error(logStr + err.Error())
			return err
		}
	}
	return nil
}

// T_HOMOGEN_MONTH contains seasonal and annual data, plus other derivative
// data combining both of these. We decided to dump only the monthly data (season BETWEEN 1 AND 12) for
//   - TAM (mean hourly temperature), and
//   - RR (hourly precipitations, note that in Stinfosys this parameter is 'RR_1')
//
// We calculate the other data on the fly (outside this program) if needed.
func dumpHomogenMonth(path string, args dumpArgs, logStr string, overwrite bool, pool *pgxpool.Pool) error {
	query := fmt.Sprintf(
		`SELECT dato AS time, %s[1]s AS data, '' AS flag FROM T_HOMOGEN_MONTH 
        WHERE %s[1]s IS NOT NULL AND stnr = $1 AND season BETWEEN 1 AND 12`,
		// NOTE: adding a dummy argument is the only way to suppress this stupid warning
		args.element, "",
	)

	rows, err := pool.Query(context.TODO(), query, args.station)
	if err != nil {
		slog.Error(logStr + err.Error())
		return err
	}

	filename := filepath.Join(path, args.element+".csv")
	if err := writeToCsv(filename, rows); err != nil {
		slog.Error(logStr + err.Error())
		return err
	}

	return nil
}

// This function is used to dump tables that don't have a FLAG table,
// (T_METARDATA, T_HOMOGEN_DIURNAL)
func dumpDataOnly(path string, args dumpArgs, logStr string, overwrite bool, pool *pgxpool.Pool) error {
	query := fmt.Sprintf(
		`SELECT dato AS time, %[1]s AS data, '' AS flag FROM %[2]s 
        WHERE %[1]s IS NOT NULL AND stnr = $1`,
		args.element,
		args.dataTable,
	)

	rows, err := pool.Query(context.TODO(), query, args.station)
	if err != nil {
		slog.Error(logStr + err.Error())
		return err
	}

	filename := filepath.Join(path, args.element+".csv")
	if err := writeToCsv(filename, rows); err != nil {
		slog.Error(logStr + err.Error())
		return err
	}

	return nil
}

// This is the default dump function.
// It selects both data and flag tables for a specific (station, element) pair,
// and then performs a full outer join on the two subqueries
func dumpDataAndFlags(path string, args dumpArgs, logStr string, overwrite bool, pool *pgxpool.Pool) error {
	query := fmt.Sprintf(
		`SELECT
            dato AS time,
            d.%[1]s AS data,
            f.%[1]s AS flag
        FROM
            (SELECT dato, %[1]s FROM %[2]s WHERE %[1]s IS NOT NULL AND stnr = $1) d
        FULL OUTER JOIN
            (SELECT dato, %[1]s FROM %[3]s WHERE %[1]s IS NOT NULL AND stnr = $1) f
        USING (dato)`,
		args.element,
		args.dataTable,
		args.flagTable,
	)

	rows, err := pool.Query(context.TODO(), query, args.station)
	if err != nil {
		slog.Error(logStr + err.Error())
		return err
	}

	filename := filepath.Join(path, args.element+".csv")
	if err := writeToCsv(filename, rows); err != nil {
		if !errors.Is(err, EMPTY_QUERY_ERR) {
			slog.Error(logStr + err.Error())
		}
		return err
	}

	return nil
}

// Dumps queried rows to file
func writeToCsv(filename string, rows pgx.Rows) error {
	lines, err := sortRows(rows)
	if err != nil {
		return err
	}

	// Return if query was empty
	if len(lines) == 0 {
		return EMPTY_QUERY_ERR
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	err = writeElementFile(lines, file)
	if closeErr := file.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

// Scans the rows and collects them in a slice of chronologically sorted lines
func sortRows(rows pgx.Rows) ([]Record, error) {
	defer rows.Close()

	records, err := pgx.CollectRows(rows, pgx.RowToStructByName[Record])
	if err != nil {
		return nil, errors.New("Could not collect rows: " + err.Error())
	}

	slices.SortFunc(records, func(a, b Record) int {
		return a.Time.Compare(b.Time)
	})

	return records, rows.Err()
}

// Writes queried (time | data | flag) columns to CSV
func writeElementFile(lines []Record, file io.Writer) error {
	// Write number of lines as header
	file.Write([]byte(fmt.Sprintf("%v\n", len(lines))))

	writer := csv.NewWriter(file)

	record := make([]string, 3)
	for _, l := range lines {
		record[0] = l.Time.Format(TIMEFORMAT)
		record[1] = l.Data.String
		record[2] = l.Flag.String

		if err := writer.Write(record); err != nil {
			return errors.New("Could not write to file: " + err.Error())
		}
	}

	writer.Flush()
	return writer.Error()
}
