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
		"SELECT standard_hlevel, hlevel_scale, paramid FROM param WHERE standard_hlevel is not null",
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

func (levels ParamLevelMap) GetLevel(paramid int32, lvl *int32) *int32 {
	// In practice level should always be different than NULL
	// due to default values in legacy systems
	if lvl == nil {
		return lvl
	}

	paramLevel, ok := levels[paramid]
	if !ok {
		return nil
	}

	level := *lvl
	if level == 0 {
		level = paramLevel.Hlevel
	}

	switch paramLevel.Scale {
	case 0:
		// level is in m, convert to cm
		level = level * 100
		return &level
	case -2:
		// level is in cm so we don't need to convert it
		return &level
	default:
		// TODO: this should return an error? And maybe add the scale in the message?
		fmt.Println("found a scale that was not 0 or -2, eeek!!!")
		return &level
	}
}
