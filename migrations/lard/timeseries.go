package lard

import (
	"context"
	"migrate/utils"
	"time"

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

func (label *Label) CreateKDVHTimeseries(element, table_name string, timespan utils.TimeSpan, pool *pgxpool.Pool) (tsid int64, err error) {
	deactivated := false
	if timespan.To != nil {
		deactivated = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	transaction, err := pool.Begin(ctx)
	if err != nil {
		return tsid, err
	}
	defer transaction.Rollback(ctx)

	// Insert new timeseries even if label exists already in LARD
	// These timeseries will be merged by a content manager
	err = transaction.QueryRow(
		ctx,
		`INSERT INTO public.timeseries (fromtime, totime, deactivated) VALUES ($1, $2, $3) RETURNING id`,
		timespan.From, timespan.To, deactivated,
	).Scan(&tsid)
	if err != nil {
		return tsid, err
	}

	_, err = transaction.Exec(
		ctx,
		`INSERT INTO labels.kdvh (timeseries, station_id, type_id, lvl, sensor, elem_code, tbl_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tsid, label.StationID, label.TypeID, label.Level, label.Sensor, element, table_name)
	if err != nil {
		return tsid, err
	}

	_, err = transaction.Exec(
		ctx,
		`INSERT INTO labels.met (timeseries, station_id, param_id, type_id, lvl, sensor)
            VALUES ($1, $2, $3, $4, $5, $6)`,
		tsid, label.StationID, label.ParamID, label.TypeID, label.Level, label.Sensor)
	if err != nil {
		return tsid, err
	}

	err = transaction.Commit(ctx)
	return tsid, err
}

func (label *Label) CreateKvalobsTimeseries(import_ts, timespan utils.TimeSpan, pool *pgxpool.Pool) (tsid int64, err error) {
	deactivated := false
	if timespan.To != nil {
		deactivated = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	transaction, err := pool.Begin(ctx)
	if err != nil {
		return tsid, err
	}
	defer transaction.Rollback(ctx)

	// Insert new timeseries even if label exists already in LARD
	// These timeseries will be merged by a content manager
	err = transaction.QueryRow(ctx,
		`INSERT INTO public.timeseries (fromtime, totime, deactivated)
            VALUES ($1, $2, $3) 
            RETURNING id`,
		timespan.From, timespan.To, deactivated,
	).Scan(&tsid)
	if err != nil {
		return tsid, err
	}

	_, err = transaction.Exec(
		ctx,
		`INSERT INTO labels.kvalobs (timeseries, station_id, param_id, type_id, lvl, sensor, import_from, import_to)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		tsid, label.StationID, label.ParamID, label.TypeID, label.Level, label.Sensor, import_ts.From, import_ts.To)
	if err != nil {
		return tsid, err
	}

	_, err = transaction.Exec(
		ctx,
		`INSERT INTO labels.met (timeseries, station_id, param_id, type_id, lvl, sensor)
            VALUES ($1, $2, $3, $4, $5, $6)`,
		tsid, label.StationID, label.ParamID, label.TypeID, label.Level, label.Sensor)
	if err != nil {
		return tsid, err
	}

	err = transaction.Commit(ctx)
	return tsid, err
}
