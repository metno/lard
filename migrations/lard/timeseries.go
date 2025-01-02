package lard

import (
	"context"
	"migrate/utils"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Struct that mimics `labels.met` table structure
type Label struct {
	StationID int32
	ParamID   int32
	TypeID    int32
	Sensor    *int32
	Level     *int32
}

// Insert timeseries with given label and timespan, returning the timeseries ID
func GetTimeseriesID(label *Label, timespan utils.TimeSpan, pool *pgxpool.Pool) (tsid int64, err error) {
	transaction, err := pool.Begin(context.TODO())
	if err != nil {
		return tsid, err
	}
	defer transaction.Rollback(context.TODO())

	var deactivated bool
	if timespan.To == nil {
		deactivated = true
	}

	err = transaction.QueryRow(
		context.TODO(),
		`INSERT INTO public.timeseries (fromtime, totime, deactivated) VALUES ($1, $2, $3) RETURNING id`,
		timespan.From, timespan.To, deactivated,
	).Scan(&tsid)
	if err != nil {
		return tsid, err
	}

	_, err = transaction.Exec(
		context.TODO(),
		`INSERT INTO labels.met (timeseries, station_id, param_id, type_id, lvl, sensor)
            VALUES ($1, $2, $3, $4, $5, $6)`,
		tsid, label.StationID, label.ParamID, label.TypeID, label.Level, label.Sensor)
	if err != nil {
		return tsid, err
	}

	err = transaction.Commit(context.TODO())
	return tsid, err
}
