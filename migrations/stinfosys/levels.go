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
	Hlevel int32
	Scale  int32
	Htype  *string
}

func (p *ParamLevel) IsNegative() bool {
	return p.Htype != nil && strings.Contains(strings.ToLower(*p.Htype), "below")
}

func CacheParamLevels(conn *pgx.Conn) ParamLevelMap {
	fmt.Printf("%50s", "Caching StinfoSys param table... ")
	cache := make(ParamLevelMap)

	rows, err := conn.Query(
		context.TODO(),
		`SELECT paramid, standard_hlevel, hlevel_scale, sensorlevel_id FROM param
		JOIN element_info ON param.element_id = element_info.element_id
		WHERE standard_hlevel IS NOT NULL
		AND hlevel_scale IS NOT NULL`,
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

		if level.Hlevel < 0 {
			level.Hlevel *= -1
		}

		switch level.Scale {
		case 0, -2:
		default:
			fmt.Println("Found invalid scale:", level.Scale)
			continue
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

func (levels ParamLevelMap) GetLevel(paramid int32, legacyLevel *int32) *int32 {
	paramLevel, ok := levels[paramid]
	// return if level was not found in the map or the legacy level is NULL
	if !ok || legacyLevel == nil {
		return nil
	}

	level := *legacyLevel
	if level == 0 {
		level = paramLevel.Hlevel
	}

	if paramLevel.Scale == 0 {
		// level is in m, convert to cm
		level *= 100
	}

	if paramLevel.IsNegative() {
		level *= -1
	}

	return &level
}
