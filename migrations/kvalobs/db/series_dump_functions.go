package db

import (
	"context"
	"fmt"
	"log/slog"
	"migrate/utils"
	"os"
	"path/filepath"

	"github.com/gocarina/gocsv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func dumpDataSeries(label *Label, timespan *utils.TimeSpan, path string, pool *pgxpool.Pool) error {
	// NOTE: sensor and level could be NULL, but in reality they have default values
	query := `SELECT obstime, original, tbtime, corrected, controlinfo, useinfo, cfailed
                FROM data
                WHERE stationid = $1
                  AND typeid = $2
                  AND paramid = $3
                  AND sensor = $4
                  AND level = $5
                  AND ($6::timestamp IS NULL OR obstime >= $6)
                  AND ($7::timestamp IS NULL OR obstime < $7)
                ORDER BY obstime`

	// Convert to string because `sensor` in Kvalobs is a BPCHAR(1)
	var sensor *string
	if label.Sensor != nil {
		sensorval := fmt.Sprint(*label.Sensor)
		sensor = &sensorval
	}

	rows, err := pool.Query(
		context.TODO(),
		query,
		label.StationID,
		label.TypeID,
		label.ParamID,
		sensor,
		label.Level,
		timespan.From,
		timespan.To,
	)
	if err != nil {
		slog.Error(label.LogStr() + err.Error())
		return err
	}

	data, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[DataObs])
	if err != nil {
		slog.Error(label.LogStr() + err.Error())
		return err
	}

	if len(data) == 0 {
		slog.Info(label.LogStr() + "No data for this label")
		return nil
	}

	return writeSeriesCSV(data, path, label)
}

func dumpTextSeries(label *Label, timespan *utils.TimeSpan, path string, pool *pgxpool.Pool) error {
	query := `SELECT obstime, original, tbtime FROM text_data
                WHERE stationid = $1
                  AND typeid = $2
                  AND paramid = $3
                  AND ($4::timestamp IS NULL OR obstime >= $4)
                  AND ($5::timestamp IS NULL OR obstime < $5)
                ORDER BY obstime`

	rows, err := pool.Query(
		context.TODO(),
		query,
		label.StationID,
		label.TypeID,
		label.ParamID,
		timespan.From,
		timespan.To,
	)
	if err != nil {
		slog.Error(label.LogStr() + err.Error())
		return err
	}

	data, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[TextObs])
	if err != nil {
		slog.Error(label.LogStr() + err.Error())
		return err
	}

	if len(data) == 0 {
		slog.Info(label.LogStr() + "No data for this label")
		return nil
	}

	return writeSeriesCSV(data, path, label)
}

func writeSeriesCSV[S DataSeries | TextSeries](series S, path string, label *Label) error {
	filename := filepath.Join(path, label.ToFilename())
	file, err := os.Create(filename)
	if err != nil {
		slog.Error(err.Error())
		return err
	}

	// Write number of lines on first line, keep headers on 2nd line
	file.Write([]byte(fmt.Sprintf("%v\n", len(series))))
	if err = gocsv.Marshal(series, file); err != nil {
		slog.Error(err.Error())
		return err
	}

	return nil
}
