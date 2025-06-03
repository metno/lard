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

type Unit = int32

const (
	METER      Unit = 0
	CENTIMETER Unit = -2
)

type Direction = int32

const (
	UP Direction = iota
	DOWN
	MISSING
)

type ParamLevel struct {
	Hlevel    int32
	Unit      Unit
	Direction Direction
}

func CacheParamLevels(conn *pgx.Conn) ParamLevelMap {
	fmt.Printf("%-50s", "Caching StinfoSys param table... ")
	cache := make(ParamLevelMap)

	rows, err := conn.Query(
		context.TODO(),
		`SELECT paramid, standard_hlevel, hlevel_scale, sensorlevel_id FROM param
		JOIN element_info ON param.element_id = element_info.element_id
		WHERE hlevel_scale IS NOT NULL`,
	)
	if err != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}

	for rows.Next() {
		var level ParamLevel
		var param ParamId
		var sensorlevelId *string
		var standardHlevel *int32

		if err := rows.Scan(&param, &standardHlevel, &level.Unit, &sensorlevelId); err != nil {
			fmt.Println("\n", err)
			os.Exit(1)
		}

		// Set Hlevel if standardHlevel is not NULL, otherwise default to 0
		if standardHlevel != nil {
			level.Hlevel = *standardHlevel
		}

		switch level.Unit {
		case METER, CENTIMETER:
		default:
			fmt.Println("Found invalid scale:", level.Unit)
			continue
		}

		if sensorlevelId != nil {
			if strings.Contains(*sensorlevelId, "below") {
				level.Direction = DOWN
			} else {
				level.Direction = UP
			}
		} else {
			level.Direction = MISSING
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

func (levels ParamLevelMap) GetLevel(paramid int32, legacyLevel int32) *int32 {
	paramLevel, ok := levels[paramid]
	// return if level was not found in the map
	if !ok {
		return nil
	}

	level := legacyLevel
	if level == 0 {
		level = paramLevel.Hlevel
	}

	// level is in m, convert to cm
	if paramLevel.Unit == METER {
		level *= 100
	}

	if paramLevel.Direction == DOWN {
		if level > 0 { // in case it was already signed...
			level *= -1
		}
	}

	return &level
}
