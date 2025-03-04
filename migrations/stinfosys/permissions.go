package stinfosys

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

const STINFO_ENV_VAR string = "STINFO_CONN_STRING"

type StationId = int32
type PermitId = int32

type ParamPermitMap map[StationId][]ParamPermit
type StationPermitMap map[StationId]PermitId

type ParamPermit struct {
	TypeId   int32
	ParamdId int32
	PermitId int32
}

type PermitMaps struct {
	ParamPermits   ParamPermitMap
	StationPermits StationPermitMap
}

func NewPermitTables(conn *pgx.Conn) PermitMaps {
	return PermitMaps{
		ParamPermits:   cacheParamPermits(conn),
		StationPermits: cacheStationPermits(conn),
	}
}

func cacheParamPermits(conn *pgx.Conn) ParamPermitMap {
	fmt.Printf("%50s", "Caching StinfoSys v_station_param_policy table... ")
	cache := make(ParamPermitMap)

	rows, err := conn.Query(
		context.TODO(),
		"SELECT stationid, message_formatid, paramid, permitid FROM v_station_param_policy",
	)
	if err != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}

	for rows.Next() {
		var stnr StationId
		var permit ParamPermit

		if err := rows.Scan(&stnr, &permit.TypeId, &permit.ParamdId, &permit.PermitId); err != nil {
			fmt.Println("\n", err)
			os.Exit(1)
		}

		cache[stnr] = append(cache[stnr], permit)
	}

	if rows.Err() != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}

	fmt.Println("Done!")
	return cache
}

func cacheStationPermits(conn *pgx.Conn) StationPermitMap {
	fmt.Printf("%-50s", "Caching StinfoSys station_policy table... ")
	cache := make(StationPermitMap)

	rows, err := conn.Query(
		context.TODO(),
		"SELECT stationid, permitid FROM station_policy",
	)
	if err != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}

	for rows.Next() {
		var stnr StationId
		var permit PermitId

		if err := rows.Scan(&stnr, &permit); err != nil {
			fmt.Println("\n", err)
			os.Exit(1)
		}

		cache[stnr] = permit
	}

	if rows.Err() != nil {
		fmt.Println("\n", rows.Err())
		os.Exit(1)
	}

	fmt.Println("Done!")
	return cache
}

func (permits *PermitMaps) TimeseriesIsOpen(stnr, typeid, paramid int32) bool {
	// First check param permit table
	if permits, ok := permits.ParamPermits[stnr]; ok {
		for _, permit := range permits {
			if (permit.TypeId == 0 || permit.TypeId == typeid) &&
				(permit.ParamdId == 0 || permit.ParamdId == paramid) {
				return permit.PermitId == 1
			}
		}
	}

	// Otherwise check station permit table
	if permit, ok := permits.StationPermits[stnr]; ok {
		return permit == 1
	}

	return false
}

// Basically the same as TimeseriesIsOpen, but with nil typeid and paramid
func (permits *PermitMaps) StationIsOpen(stnr int32) bool {
	// First check param permit table
	if permits, ok := permits.ParamPermits[stnr]; ok {
		for _, permit := range permits {
			if permit.TypeId == 0 && permit.ParamdId == 0 {
				return permit.PermitId == 1
			}
		}
	}

	// Otherwise check station permit table
	if permit, ok := permits.StationPermits[stnr]; ok {
		return permit == 1
	}

	return false
}
