package port

import (
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kvalobs/db"
	"migrate/lard"
)

// NOTE:
// - for both kvalobs and histkvalobs:
//      - all stinfo non-scalar params that can be found in Kvalobs are stored in `text_data`
//      - 305, 306, 307, 308 are also in `data` but should be treated as `text_data` -> Special Cloud Types
//          => should always use readDataCSV and lard.InsertData for these
// - only for histkvalobs
//      - 2751, 2752, 2753, 2754 are in `text_data` but should be treated as `data`? -> Metar Cloud types

func importData(file *os.File, tsid int64, label *db.Label, logStr string, pool *pgxpool.Pool) (int64, error) {
	if label.IsSpecialCloudType() {
		text, err := parseSpecialCloudType(tsid, file)
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

	data, flags, err := parseDataCSV(tsid, file)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}

	count, err := lard.InsertData(data, pool, logStr)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}

	if err := lard.InsertLegacyFlags(flags, pool, logStr); err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}

	return count, nil
}

func importText(file *os.File, tsid int64, label *db.Label, logStr string, pool *pgxpool.Pool) (int64, error) {
	if label.IsMetarCloudType() {
		data, err := parseMetarCloudType(tsid, file)
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

	text, err := parseTextCSV(tsid, file)
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
