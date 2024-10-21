package main

import (
	"context"
	"fmt"
	"kdvh_importer/kdvh"

	// "os"
	"testing"
	// "time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/rickb777/period"
)

const LARD_STRING string = "host=localhost user=postgres dbname=postgres password=postgres"

func mockConfig(elem, table string, station int64) *kdvh.MigrateConfig {
	paramKey := kdvh.ParamKey{
		ElemCode:  elem,
		TableName: table,
	}

	key := kdvh.KDVHKey{ParamKey: paramKey, Station: station}

	return &kdvh.MigrateConfig{
		Tables:    []string{table},
		Stations:  []string{fmt.Sprint(station)},
		Elements:  []string{elem},
		BaseDir:   "./tables",
		HasHeader: true,
		Sep:       ";",
		OffsetMap: map[kdvh.ParamKey]period.Period{paramKey: {}},
		// StinfoMap: map[kdvh.ParamKey]kdvh.Metadata{paramKey: {
		// ElemCode:  elem,
		// TableName: table,
		// TypeID:    0,
		// ParamID:   0,
		// Hlevel:    0,
		// Sensor:    0,
		// Fromtime: &time.Time{},
		// }},
		KDVHMap: map[kdvh.KDVHKey]*kdvh.MetaKDVH{key: {
			TableName: table,
			Station:   station,
			ElemCode:  elem,
			// FromTime:  &time.Time{},
			// ToTime:    &time.Time{},
		}},
	}

}

func TestImportT_MDATA(t *testing.T) {
	err := godotenv.Load()
	if err != nil {
		fmt.Println(err)
		return
	}

	db := kdvh.Init()

	var station int64 = 77062
	elem := "TA"
	testTable := "T_MDATA"

	config := mockConfig(elem, testTable, station)
	config.StinfoMap = db.CacheStinfo(config.Tables, config.Elements)

	pool, err := pgxpool.New(context.TODO(), LARD_STRING)
	if err != nil {
		t.Log("Could not connect to Lard:", err)
	}
	defer pool.Close()

	for _, table := range db.Tables {
		if testTable == table.TableName {
			table.SetImport(3000).Import(pool, config)
		}
	}
}
