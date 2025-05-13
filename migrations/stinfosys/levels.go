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
	Hlevel       int32
	Hlevel_scale int32
}

func CacheParamLevels(conn *pgx.Conn) ParamLevelMap {
	fmt.Printf("%50s", "Caching StinfoSys param table... ")
	cache := make(ParamLevelMap)

	rows, err := conn.Query(
		context.TODO(),
		"SELECT standard_hlevel, hlevel_scale, paramid FROM param WHERE standard_hlevel is not null",
	)
	if err != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}

	for rows.Next() {
		var param ParamId
		var level ParamLevel

		if err := rows.Scan(&param, &level.Hlevel, &level.Hlevel_scale); err != nil {
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

func (levels ParamLevelMap) GetLevel(paramid, lvl int32) *int32 {
	var level = lvl
	// First check param permit table
	if level_and_scale, ok := levels[paramid]; ok {
		if lvl == 0 {
			// override with default
			level = level_and_scale.Hlevel
		}
		// convert to cm
		if level_and_scale.Hlevel_scale == 0 {
			// was m, convert to cm
			level = level * 100
			return &level
		} else if level_and_scale.Hlevel_scale == -2 {
			// is cm so do nothing
			return &level
		} else {
			fmt.Println("found a scale that was not 0 or -2, eeek!!!")
			return &level
		}
	}

	return nil
}
