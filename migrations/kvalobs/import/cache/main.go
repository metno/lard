package cache

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"

	"migrate/kvalobs/db"
	"migrate/stinfosys"
	"migrate/utils"
)

type KvalobsTimespanMap = map[MetaKey]utils.TimeSpan

type Cache struct {
	Meta    KvalobsTimespanMap
	Permits stinfosys.PermitMaps
	// Params  stinfosys.ScalarMap // Don't need them
}

func New(kvalobs *db.DB) *Cache {
	conn, ctx := stinfosys.Connect()
	defer conn.Close(ctx)

	permits := stinfosys.NewPermitTables(conn)
	// timeseries :=

	timespans := cacheKvalobsTimeseriesTimespans(kvalobs)
	return &Cache{Permits: permits, Meta: timespans}
}

func (c *Cache) GetSeriesTimespan(label *db.Label) (utils.TimeSpan, error) {
	// First try to lookup timespan with both stationid and paramid
	// TODO: should these timespans modify an existing timeseries in lard?
	key := MetaKey{Stationid: label.StationID, Paramid: sql.NullInt32{Int32: label.ParamID, Valid: true}}
	if timespan, ok := c.Meta[key]; ok {
		return timespan, nil
	}

	// Otherwise try with stationid only
	key.Paramid = sql.NullInt32{}
	if timespan, ok := c.Meta[key]; ok {
		return timespan, nil
	}

	// If there is no timespan we insert null fromtime and totime
	// TODO: is this really what we want to do?
	// Is there another place where to find this information?
	return utils.TimeSpan{}, nil
}

func (c *Cache) TimeseriesIsOpen(stnr, typeid, paramid int32) bool {
	return c.Permits.TimeseriesIsOpen(stnr, typeid, paramid)
}

// In `station_metadata` only the stationid is required to be non-NULL
// Paramid can be optionally specified
// Typeid, sensor, and level column are all NULL, so they are not present in this struct
type MetaKey struct {
	Stationid int32
	Paramid   sql.NullInt32
}

// Query kvalobs `station_metadata` table that stores timeseries timespans
func cacheKvalobsTimeseriesTimespans(kvalobs *db.DB) KvalobsTimespanMap {
	cache := make(KvalobsTimespanMap)

	slog.Info("Connecting to Kvalobs to cache metadata")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv(kvalobs.ConnEnvVar))
	if err != nil {
		slog.Error("Could not connect to Kvalobs. Make sure to be connected to the VPN. " + err.Error())
		os.Exit(1)
	}
	defer conn.Close(ctx)

	query := `SELECT stationid, paramid, fromtime, totime FROM station_metadata`

	rows, err := conn.Query(context.TODO(), query)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	for rows.Next() {
		var key MetaKey
		var timespan utils.TimeSpan

		err := rows.Scan(
			&key.Stationid,
			&key.Paramid,
			&timespan.From,
			&timespan.To,
		)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		cache[key] = timespan
	}

	if rows.Err() != nil {
		slog.Error(rows.Err().Error())
		os.Exit(1)
	}

	return cache
}
