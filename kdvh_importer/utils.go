package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"time"

	"github.com/go-gota/gota/dataframe"
)

// Writes whole table to csv file with 'sep' separator
func writeRows(rows *sql.Rows, writer io.WriteCloser, sep rune) error {
	defer rows.Close()
	defer writer.Close()

	csvWriter := csv.NewWriter(writer)
	csvWriter.Comma = sep

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	err = csvWriter.Write(columns)
	if err != nil {
		return fmt.Errorf("Could not write headers: %w", err)
	}

	count := len(columns)
	values := make([]interface{}, count)
	pointers := make([]interface{}, count)

	for rows.Next() {
		row := make([]string, count)

		for i := range columns {
			pointers[i] = &values[i]
		}

		if err = rows.Scan(pointers...); err != nil {
			return err
		}

		floatFormat := "%.2f"
		timeFormat := "2006-01-02_15:04:05"
		for i := range columns {
			var value string

			switch v := values[i].(type) {
			case []byte:
				value = string(v)
			case float64, float32:
				value = fmt.Sprintf(floatFormat, v)
			case time.Time:
				value = v.Format(timeFormat)
			default:
				value = fmt.Sprintf("%v", v)
			}

			row[i] = value
		}

		err = csvWriter.Write(row)
		if err != nil {
			return fmt.Errorf("Could not write row to csv: %w", err)
		}
	}

	err = rows.Err()
	csvWriter.Flush()
	return err
}

// func readFile(filename string, sep string) ([][]string, error) {
func readFile(filename string, sep string) (dataframe.DataFrame, error) {
	file, err := os.Open(filename)
	if err != nil {
		return dataframe.DataFrame{}, err
	}
	defer file.Close()

	// TODO: dump header
	return dataframe.ReadCSV(file, dataframe.HasHeader(false), dataframe.WithDelimiter([]rune(sep)[0])), nil

	// reader := csv.NewReader(file)

	// NOTE: we already asserted sep is a single char
	// reader.Comma = []rune(sep)[0]
	// return reader.ReadAll()
}

// Filters elements of a slice by comparing them to the elements of a reference slice
func filterSlice(list, reference []string) []string {
	if list == nil {
		return reference
	}

	var out []string
	for _, s := range list {
		if !slices.Contains(reference, s) {
			log.Printf("User provided input '%s' is not present in the database", s)
			continue
		}
		out = append(out, s)
	}
	return out
}

func setLogFile(tableName, procedure string) {
	filename := fmt.Sprintf("%s_%s_log.txt", tableName, procedure)
	fh, err := os.Create(filename)
	if err != nil {
		log.Printf("Could not create log '%s': %s", filename, err)
		return
	}
	log.SetOutput(fh)
}

// printRows for testing
func printRows(rows *sql.Rows) error {
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	fmt.Println(columns)

	count := len(columns)
	values := make([]interface{}, count)
	pointers := make([]interface{}, count)

	for rows.Next() {
		for i := range columns {
			pointers[i] = &values[i]
		}

		if err = rows.Scan(pointers...); err != nil {
			return err
		}

		floatFormat := "%.2f"
		timeFormat := "2006-01-02_15:04:05"
		for i := range columns {
			switch v := values[i].(type) {
			case []byte:
				values[i] = string(v)
			case float64, float32:
				values[i] = fmt.Sprintf(floatFormat, v)
			case time.Time:
				values[i] = v.Format(timeFormat)
			default:
				values[i] = fmt.Sprintf("%v", v)
			}
		}

		fmt.Println(values)
	}

	err = rows.Err()
	return err
}
