package stinfosys

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

// Map of metadata used to query timeseries ID in LARD
type ElemMap = map[Key]Param

// Key is used for lookup of parameter offsets and metadata from Stinfosys
type Key struct {
	ElemCode  string
	TableName string
}

// Subset of elem_map_cfnames_param query with only param info
type Param struct {
	TypeID   int32
	ParamID  int32
	Hlevel   *int32
	Sensor   int32
	Fromtime time.Time
	IsScalar bool
}

// Save metadata for later use by quering Stinfosys
func CacheElemMap(conn *pgx.Conn) ElemMap {
	fmt.Print("Caching StinfoSys elem_map_cfnames_param table... ")
	cache := make(ElemMap)

	rows, err := conn.Query(
		context.TODO(),
		`SELECT elem_code, table_name, typeid, paramid, hlevel, sensor, fromtime, scalar
            FROM elem_map_cfnames_param
            JOIN param USING(paramid)`,
	)
	if err != nil {
		fmt.Println("\n", err)
		os.Exit(1)
	}

	for rows.Next() {
		var key Key
		var param Param
		err := rows.Scan(
			&key.ElemCode,
			&key.TableName,
			&param.TypeID,
			&param.ParamID,
			&param.Hlevel,
			&param.Sensor,
			&param.Fromtime,
			&param.IsScalar,
		)
		if err != nil {
			fmt.Println("\n", err)
			os.Exit(1)
		}

		cache[key] = param
	}

	if rows.Err() != nil {
		fmt.Println("\n", rows.Err())
		os.Exit(1)
	}

	fmt.Println("Done!")
	return cache
}
