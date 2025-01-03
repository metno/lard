package cache

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	kdvh "migrate/kdvh/db"
	"migrate/lard"
	"migrate/stinfosys"
	"migrate/utils"
)

type Cache struct {
	Offsets   OffsetMap
	Timespans KDVHMap
	Elements  stinfosys.ElemMap
	Permits   stinfosys.PermitMaps
}

// Caches all the metadata needed for import of KDVH tables.
// If any error occurs inside here the program will exit.
func CacheMetadata(tables, stations, elements []string, database *kdvh.KDVH) *Cache {
	stconn, ctx := stinfosys.Connect()
	defer stconn.Close(ctx)

	return &Cache{
		Elements:  stinfosys.CacheElemMap(stconn),
		Permits:   stinfosys.NewPermitTables(stconn),
		Offsets:   cacheParamOffsets(),
		Timespans: cacheKDVH(tables, stations, elements, database),
	}
}

func (cache *Cache) NewTsInfo(table, element string, station int32, pool *pgxpool.Pool) (*kdvh.TsInfo, error) {
	logstr := fmt.Sprintf("[%v - %v - %v]: ", table, station, element)
	key := newKDVHKey(element, table, station)

	param, ok := cache.Elements[key.Inner]
	if !ok {
		// TODO: should it fail here? How do we deal with data without metadata?
		slog.Error(logstr + "Missing metadata in Stinfosys")
		return nil, errors.New("No metadata")
	}

	// Check if data for this station/element is restricted
	// TODO: eventually use this to choose which table to use on insert
	isOpen := cache.Permits.TimeseriesIsOpen(station, param.TypeID, param.ParamID)
	if !isOpen {
		slog.Warn(logstr + "Timeseries data is restricted")
		return nil, errors.New("Restricted data")
	}

	// No need to check for `!ok`, will default to 0 offset
	offset := cache.Offsets[key.Inner]

	// No need to check for `!ok`, timespan will be ignored if not in the map
	timespan, ok := cache.Timespans[key]

	label := lard.Label{
		StationID: station,
		TypeID:    param.TypeID,
		ParamID:   param.ParamID,
		Sensor:    &param.Sensor,
		Level:     param.Hlevel,
	}

	// TODO: are Param.Fromtime and Span.From different?
	slog.Info(fmt.Sprintf("stinfo.fromtime %v - kdvh.fromtime - %v", param.Fromtime, timespan.From))
	tsid, err := lard.GetTimeseriesID(&label, utils.TimeSpan{From: &param.Fromtime, To: timespan.To}, pool)
	if err != nil {
		slog.Error(logstr + "could not obtain timeseries - " + err.Error())
		return nil, err
	}

	return &kdvh.TsInfo{
		Id:       tsid,
		Station:  station,
		Element:  element,
		Offset:   offset,
		Param:    param,
		Timespan: timespan,
		Logstr:   logstr,
	}, nil
}
