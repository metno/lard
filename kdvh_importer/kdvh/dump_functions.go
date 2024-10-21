package kdvh

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type DumpFunction func(dumpFuncArgs, *sql.DB) error
type dumpFuncArgs struct {
	path      string
	element   string
	station   string
	dataTable string
	flagTable string
}

// Fetch min and max year from table, needed for tables that are dumped by year
func fetchYearRange(tableName, station string, conn *sql.DB) (int64, int64, error) {
	var beginStr, endStr string
	query := fmt.Sprintf("SELECT min(to_char(dato, 'yyyy')), max(to_char(dato, 'yyyy')) FROM %s WHERE stnr = $1", tableName)

	if err := conn.QueryRow(query, station).Scan(&beginStr, &endStr); err != nil {
		slog.Error(fmt.Sprint("Could not query row: ", err))
		return 0, 0, err
	}

	begin, err := strconv.ParseInt(beginStr, 10, 64)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not parse year '%s': %s", beginStr, err))
		return 0, 0, err
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not parse year '%s': %s", endStr, err))
		return 0, 0, err
	}

	return begin, end, nil
}

func dumpByYearDataOnly(args dumpFuncArgs, conn *sql.DB) error {
	begin, end, err := fetchYearRange(args.dataTable, args.station, conn)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		`SELECT dato AS time, %[1]s AS data FROM %[2]s \
        WHERE %[1]s IS NOT NULL AND stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2`,
		args.element,
		args.dataTable,
	)

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, args.station, year)
		if err != nil {
			slog.Error(fmt.Sprint("Could not query KDVH: ", err))
			return err
		}

		path := filepath.Join(args.path, fmt.Sprint(year))
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			continue
		}

		if err := dumpToFile(path, args.element, rows); err != nil {
			slog.Error(err.Error())
			return err
		}
	}

	return nil
}

func dumpByYear(args dumpFuncArgs, conn *sql.DB) error {
	dataBegin, dataEnd, err := fetchYearRange(args.dataTable, args.station, conn)
	if err != nil {
		return err
	}

	flagBegin, flagEnd, err := fetchYearRange(args.flagTable, args.station, conn)
	if err != nil {
		return err
	}

	begin := min(dataBegin, flagBegin)
	end := max(dataEnd, flagEnd)

	query := fmt.Sprintf(
		`SELECT
            COALESCE(d.dato, f.dato) AS time,
            d.%[1]s AS data,
            f.%[1]s AS flag
        FROM
            (SELECT dato, stnr, %[1]s FROM %[2]s
                WHERE %[1]s IS NOT NULL AND stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2) d
        FULL OUTER JOIN
            (SELECT dato, stnr, %[1]s FROM %[3]s
                WHERE %[1]s IS NOT NULL AND stnr = $1 AND TO_CHAR(dato, 'yyyy') = $2) f
        ON d.dato = f.dato`,
		args.element,
		args.dataTable,
		args.flagTable,
	)

	for year := begin; year < end; year++ {
		rows, err := conn.Query(query, args.station, year)
		if err != nil {
			slog.Error(fmt.Sprint("Could not query KDVH: ", err))
			return err
		}

		path := filepath.Join(args.path, fmt.Sprint(year))
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			slog.Error(err.Error())
			continue
		}

		if err := dumpToFile(path, args.element, rows); err != nil {
			slog.Error(err.Error())
			return err
		}
	}

	return nil
}

func dumpHomogenMonth(args dumpFuncArgs, conn *sql.DB) error {
	query := fmt.Sprintf(
		`SELECT dato AS time, %s[1]s AS data, '' AS flag FROM T_HOMOGEN_MONTH 
        WHERE %s[1]s IS NOT NULL AND stnr = $1 AND season BETWEEN 1 AND 12`,
		// NOTE: adding a dummy argument is the only way to suppress this stupid warning
		args.element, "",
	)

	rows, err := conn.Query(query, args.station)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	if err := dumpToFile(args.path, args.element, rows); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func dumpDataOnly(args dumpFuncArgs, conn *sql.DB) error {
	query := fmt.Sprintf(
		"SELECT dato AS time, %[1]s AS data, '' AS flag FROM %[2]s WHERE %[1]s IS NOT NULL AND stnr = $1",
		args.element,
		args.dataTable,
	)

	rows, err := conn.Query(query, args.station)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	if err := dumpToFile(args.path, args.element, rows); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func dumpDataAndFlags(args dumpFuncArgs, conn *sql.DB) error {
	query := fmt.Sprintf(
		`SELECT
            COALESCE(d.dato, f.dato) AS time,
            d.%[1]s AS data,
            f.%[1]s AS flag
        FROM
            (SELECT dato, %[1]s FROM %[2]s WHERE %[1]s IS NOT NULL AND stnr = $1) d
        FULL OUTER JOIN
            (SELECT dato, %[1]s FROM %[3]s WHERE %[1]s IS NOT NULL AND stnr = $1) f
            ON d.dato = f.dato`,
		args.element,
		args.dataTable,
		args.flagTable,
	)

	rows, err := conn.Query(query, args.station)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	if err := dumpToFile(args.path, args.element, rows); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}

func dumpToFile(path, element string, rows *sql.Rows) error {
	filename := filepath.Join(path, element+".csv")
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	err = writeElementFile(rows, file)
	if closeErr := file.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

// Writes queried (time | data | flag) columns to CSV
func writeElementFile(rows *sql.Rows, file io.Writer) error {
	defer rows.Close()

	floatFormat := "%.2f"
	timeFormat := "2006-01-02_15:04:05"

	columns, err := rows.Columns()
	if err != nil {
		return errors.New("Could not ingget columns: " + err.Error())
	}

	count := len(columns)
	line := make([]string, count)
	values := make([]interface{}, count)
	pointers := make([]interface{}, count)

	writer := csv.NewWriter(file)
	// writer.Comma = ';'

	for rows.Next() {
		for i := range columns {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return errors.New("Could not scan rows: " + err.Error())
		}

		// Parse scanned types
		for i := range columns {
			var value string

			switch v := values[i].(type) {
			case []byte:
				value = string(v)
			case float64, float32:
				value = fmt.Sprintf(floatFormat, v)
			case time.Time:
				value = v.Format(timeFormat)
			case nil:
				value = ""
			default:
				value = fmt.Sprintf("%v", v)
			}

			line[i] = value
		}

		if err := writer.Write(line); err != nil {
			return errors.New("Could not write to file: " + err.Error())
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}

	return rows.Err()
}
