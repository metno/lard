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

func (l *Label) sensorLevelAreBothZero() bool {
	if l.Sensor == nil || l.Level == nil {
		return false
	}
	return *l.Level == 0 && *l.Sensor == 0
}

func GetTimeseriesID(label *Label, timespan utils.TimeSpan, pool *pgxpool.Pool) (tsid int64, err error) {
	// Query LARD labels table
	err = pool.QueryRow(
		context.TODO(),
		`SELECT timeseries FROM labels.met
            WHERE station_id = $1
            AND param_id = $2
            AND type_id = $3
            AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
            AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))`,
		label.StationID, label.ParamID, label.TypeID, label.Level, label.Sensor).Scan(&tsid)

	// If timeseries exists, return its ID
	if err == nil {
		return tsid, nil
	}

	// In KDVH and Kvalobs sensor and level have default values, while in LARD they are NULL
	// if Obsinn does not specify them. Therefore we need to check if sensor and level are NULL
	// when they are both zero.
	// FIXME(?): in some cases, level and sensor are marked with (0,0) in Obsinn,
	// so there might be problems if a timeseries is not present in LARD at the time of importing
	if label.sensorLevelAreBothZero() {
		err := pool.QueryRow(
			context.TODO(),
			`SELECT timeseries FROM labels.met
                WHERE station_id = $1
                AND param_id = $2
                AND type_id = $3
                AND lvl IS NULL
                AND sensor IS NULL`,
			label.StationID, label.ParamID, label.TypeID).Scan(&tsid)

		if err == nil {
			return tsid, nil
		}
	}

	// If none of the above worked insert a new timeseries
	transaction, err := pool.Begin(context.TODO())
	if err != nil {
		return tsid, err
	}
	defer transaction.Rollback(context.TODO())

	// TODO: should we set `deactivated` to true if `totime` is not NULL?
	err = transaction.QueryRow(
		context.TODO(),
		`INSERT INTO public.timeseries (fromtime, totime) VALUES ($1, $2) RETURNING id`,
		timespan.From, timespan.To,
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
