package stinfosys

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

type ParamId = int32

type ParamLevelMap map[ParamId]ParamLevel

type ParamLevel struct {
	Hlevel int32
	Scale  int32
}

func CacheParamLevels(conn *pgx.Conn) ParamLevelMap {
	fmt.Printf("%50s", "Caching StinfoSys param table... ")
	cache := make(ParamLevelMap)

	rows, err := conn.Query(
		context.TODO(),
		"SELECT paramid, standard_hlevel, hlevel_scale FROM param WHERE standard_hlevel is not null",
	)
	if err != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}

	for rows.Next() {
		var param ParamId
		var level ParamLevel

		if err := rows.Scan(&param, &level.Hlevel, &level.Scale); err != nil {
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
		level = paramLevel.Hlevel
	}

	switch paramLevel.Scale {
	case 0:
		// level is in m, convert to cm
		level = level * 100
		return &level, nil
	case -2:
		// level is in cm so we don't need to convert it
		return &level, nil
	default:
		return nil, fmt.Errorf("Unknown level scale: %d", paramLevel.Scale)
	}
}
