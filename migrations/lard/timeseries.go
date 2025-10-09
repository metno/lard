package lard

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Struct that mimics `labels.met` table structure
type Label struct {
	StationID int32
	ParamID   int32
	TypeID    int32
	Sensor    *int32
	// Metereological level in cm
	// This also maps the default level (0) in legacy systems to the actual
	// metereological level
	Level *int32
	// Original Hlevel in legacy systems
	LegacyLvl *int32
}

func (label *Label) CreateKDVHTimeseries(element, table_name string, permit *int32, pool *pgxpool.Pool) (tsid int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	row := pool.QueryRow(ctx,
		`SELECT timeseries FROM labels.kdvh
            WHERE station_id = $1
              ANd type_id = $2
              AND lvl = $3
              AND sensor = $4
              AND elem_code = $5
              AND tbl_name = $6`,
		label.StationID, label.TypeID, label.LegacyLvl, label.Sensor, element, table_name)

	err = row.Scan(&tsid)
	if err == nil {
		return tsid, err
	}

	// Insert new timeseries if label does not already exist in LARD
	transaction, err := pool.Begin(ctx)
	if err != nil {
		return tsid, err
	}
	defer transaction.Rollback(ctx)

	err = transaction.QueryRow(ctx,
		`INSERT INTO public.timeseries (permit, deactivated) VALUES ($1, false) RETURNING id`, permit,
	).Scan(&tsid)
	if err != nil {
		return tsid, err
	}

	_, err = transaction.Exec(
		ctx,
		`INSERT INTO labels.kdvh (timeseries, station_id, type_id, lvl, sensor, elem_code, tbl_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tsid, label.StationID, label.TypeID, label.LegacyLvl, label.Sensor, element, table_name)
	if err != nil {
		return tsid, err
	}

	// for the MET label we use the converted level
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

func (label *Label) CreateKvalobsTimeseries(permit *int32, pool *pgxpool.Pool) (tsid int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	row := pool.QueryRow(ctx,
		`SELECT timeseries FROM labels.kvalobs
            WHERE station_id = $1
              AND param_id = $2
              ANd type_id = $3
              AND lvl = $4
              AND sensor = $5`,
		label.StationID, label.ParamID, label.TypeID, label.LegacyLvl, label.Sensor)

	err = row.Scan(&tsid)
	if err == nil {
		return tsid, nil
	}

	// Insert new timeseries if label does not already exist in LARD
	transaction, err := pool.Begin(ctx)
	if err != nil {
		return tsid, err
	}
	defer transaction.Rollback(ctx)

	err = transaction.QueryRow(ctx,
		`INSERT INTO public.timeseries (permit, deactivated) VALUES ($1, false) RETURNING id`, permit,
	).Scan(&tsid)
	if err != nil {
		return tsid, err
	}

	_, err = transaction.Exec(
		ctx,
		`INSERT INTO labels.kvalobs (timeseries, station_id, param_id, type_id, lvl, sensor)
            VALUES ($1, $2, $3, $4, $5, $6)`,
		tsid, label.StationID, label.ParamID, label.TypeID, label.LegacyLvl, label.Sensor)
	if err != nil {
		return tsid, err
	}

	// for the MET label we use the converted level
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
