package stinfosys

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
)

type ParamId = int32

type ParamLevelMap map[ParamId]ParamLevel

type ParamLevel struct {
	Hlevel *int32
	Scale  *int32
	Htype  *string
}

func CacheParamLevels(conn *pgx.Conn) ParamLevelMap {
	fmt.Printf("%50s", "Caching StinfoSys param table... ")
	cache := make(ParamLevelMap)

	rows, err := conn.Query(
		context.TODO(),
		"SELECT standard_hlevel, hlevel_scale, paramid, sensorlevel_id FROM param JOIN element_info ON param.element_id = element_info.element_id",
	)
	if err != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}

	for rows.Next() {
		var param ParamId
		var level ParamLevel

		if err := rows.Scan(&param, &level.Hlevel, &level.Scale, &level.Htype); err != nil {
			fmt.Println("\n", err)
			os.Exit(1)
		}

		cache[param] = level
	}

	if rows.Err() != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}

	fmt.Println("Done!")
	return cache
}

func (levels ParamLevelMap) GetLevel(paramid int32, legacyLevel *int32) (*int32, error) {
	paramLevel, ok := levels[paramid]
	// return if level was not found in the map or the legacy level is NULL
	if !ok || legacyLevel == nil {
		return nil, nil
	}

	level := *legacyLevel
	if level == 0 {
		if paramLevel.Hlevel == nil {
			return nil, nil
		}
		level = *paramLevel.Hlevel
	}

	if paramLevel.Scale == nil {
		if paramLevel.Htype != nil {
			// could be negative?
			if strings.Contains(*paramLevel.Htype, "below") {
				level = level * -1
				return &level, nil
			}
		}
		return &level, nil
	}
	switch *paramLevel.Scale {
	case 0:
		// level is in m, convert to cm
		level = level * 100
	case -2:
		// level is in cm so we don't need to convert it
	default:
		return nil, fmt.Errorf("unknown level scale: %d", paramLevel.Scale)
	}
	if paramLevel.Htype != nil {
		// could be negative?
		if strings.Contains(*paramLevel.Htype, "below") {
			level = level * -1
			return &level, nil
		}
	}
	return &level, nil
}
