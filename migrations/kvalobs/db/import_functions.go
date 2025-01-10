package db

import (
	"bufio"
	"log/slog"
	"migrate/lard"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NOTE:
// - for both kvalobs and histkvalobs:
//      - all stinfo non-scalar params that can be found in Kvalobs are stored in `text_data`
//      - 305, 306, 307, 308 are also in `data` but should be treated as `text_data`
//          => should always use readDataCSV and lard.InsertData for these
// - only for histkvalobs
//      - 2751, 2752, 2753, 2754 are in `text_data` but should be treated as `data`?

func importData(tsid int64, label *Label, filename, logStr string, pool *pgxpool.Pool) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Parse number of rows
	scanner.Scan()
	rowCount, _ := strconv.Atoi(scanner.Text())

	// Skip header
	scanner.Scan()

	if label.IsSpecialCloudType() {
		text, err := parseSpecialCloudType(tsid, rowCount, scanner)
		if err != nil {
			slog.Error(logStr + err.Error())
			return 0, err
		}

		count, err := lard.InsertTextData(text, pool, logStr)
		if err != nil {
			slog.Error(logStr + err.Error())
			return 0, err
		}

		return count, nil
	}

	data, flags, err := parseDataCSV(tsid, rowCount, scanner)
	count, err := lard.InsertData(data, pool, logStr)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}

	if err := lard.InsertFlags(flags, pool, logStr); err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}

	return count, nil
}

func importText(tsid int64, label *Label, filename, logStr string, pool *pgxpool.Pool) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Parse number of rows
	scanner.Scan()
	rowCount, _ := strconv.Atoi(scanner.Text())

	// Skip header
	scanner.Scan()

	if label.IsMetarCloudType() {
		data, err := parseMetarCloudType(tsid, rowCount, scanner)
		if err != nil {
			slog.Error(logStr + err.Error())
			return 0, err
		}
		count, err := lard.InsertData(data, pool, logStr)
		if err != nil {
			slog.Error(logStr + err.Error())
			return 0, err
		}

		return count, nil
	}

	text, err := parseTextCSV(tsid, rowCount, scanner)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}

	count, err := lard.InsertTextData(text, pool, logStr)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}

	return count, nil
}
