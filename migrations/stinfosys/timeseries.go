package stinfosys

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	kvalobs "migrate/kvalobs/db"
	"migrate/utils"
)

type TimespanMap = map[kvalobs.Label]utils.TimeSpan

func getTimeseries(conn *pgx.Conn) TimespanMap {
	cache := make(TimespanMap)

	rows, err := conn.Query(context.TODO(),
		`SELECT stationid, message_formatid, paramid, sensor, level, fromtime, totime
            FROM time_series`)
	if err != nil {
		log.Error().Err(err).Msg("")
		os.Exit(1)
	}

	for rows.Next() {
		var label kvalobs.Label
		var timespan utils.TimeSpan

		err := rows.Scan(
			&label.StationID,
			&label.TypeID,
			&label.ParamID,
			&label.Sensor,
			&label.Level,
			&timespan.From,
			&timespan.To,
		)
		if err != nil {
			log.Error().Err(err).Msg("")
			os.Exit(1)
		}

		cache[label] = timespan
	}

	return cache
}
