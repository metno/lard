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
	var deactivated bool
	if timespan.To != nil {
		deactivated = true
	}

	// err = pool.QueryRow(context.TODO(),
	// 	`SELECT tsid FROM public.timeseries
	//            WHERE fromtime = $1
	//            AND ($2::timestampz IS NULL AND totime IS NULL) OR (totime = $2)
	//            AND deactivated = $3
	//            AND id IN (
	//                SELECT timeseries FROM labels.met
	//                WHERE station_id = $4
	//                AND param_id = $5
	//                AND type_id = $6
	//                AND (($7::int IS NULL AND lvl IS NULL) OR (lvl = $7))
	//                AND (($8::int IS NULL AND sensor IS NULL) OR (sensor = $8))
	//            )`,
	// 	timespan.From, timespan.To, deactivated,
	// 	label.StationID, label.ParamID, label.TypeID, label.Level, label.Sensor,
	// ).Scan(&tsid)
	// if err == nil {
	// 	return tsid, nil
	// }

	transaction, err := pool.Begin(context.TODO())
	if err != nil {
		return tsid, err
	}
	defer transaction.Rollback(context.TODO())

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
