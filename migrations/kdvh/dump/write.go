package dump

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

// Format string for date field in CSV files
const TIMEFORMAT string = "2006-01-02_15:04:05"

// Error returned if no observations are found for a (station, element) pair
var EMPTY_QUERY_ERR error = errors.New("The query did not return any rows")

// Struct representing a single record in the output CSV file
type Record struct {
	Time   time.Time      `db:"time"`
	TypeID sql.NullString `db:"typeid"`
	Data   sql.NullString `db:"data"`
	Flag   sql.NullString `db:"flag"`
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

// Scans the rows and collects them in a slice of chronologically sorted records
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

// Writes queried (time | type ID | data | flag) columns to CSV
func writeElementFile(lines []Record, file io.Writer) error {
	// Write number of lines as header
	file.Write(fmt.Appendf(nil, "%v\n", len(lines)))

	writer := csv.NewWriter(file)

	record := make([]string, 4)
	for _, l := range lines {
		record[0] = l.Time.Format(TIMEFORMAT)
		record[1] = l.TypeID.String
		record[2] = l.Data.String
		record[3] = l.Flag.String

		if err := writer.Write(record); err != nil {
			return errors.New("Could not write to file: " + err.Error())
		}
	}

	writer.Flush()
	return writer.Error()
}
