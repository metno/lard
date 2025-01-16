package lard

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func InsertData(ts [][]any, pool *pgxpool.Pool, logStr string) (int64, error) {
	size := len(ts)
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromRows(ts),
	)
	if err != nil {
		return count, err
	}

	logStr += fmt.Sprintf("%v/%v data rows inserted", count, size)
	if int(count) != size {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return count, nil
}

func InsertTextData(ts [][]any, pool *pgxpool.Pool, logStr string) (int64, error) {
	size := len(ts)
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "nonscalar_data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromRows(ts),
	)
	if err != nil {
		return count, err
	}

	logStr += fmt.Sprintf("%v/%v non-scalar data rows inserted", count, size)
	if int(count) != size {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return count, nil
}

// TODO: maybe this should also return a insert count for testing purposes
func InsertFlags(ts [][]any, pool *pgxpool.Pool, logStr string) error {
	size := len(ts)
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"flags", "kvdata"},
		[]string{"timeseries", "obstime", "original", "corrected", "controlinfo", "useinfo", "cfailed"},
		pgx.CopyFromRows(ts),
	)
	if err != nil {
		return err
	}

	logStr += fmt.Sprintf("%v/%v flag rows inserted", count, size)
	if int(count) != size {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return nil
}
