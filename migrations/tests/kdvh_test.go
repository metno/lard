package tests

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"migrate/kdvh/db"
	port "migrate/kdvh/import"
	"migrate/kdvh/import/cache"
	"migrate/stinfosys"
)

type KdvhTestCase struct {
	table        string
	station      int32
	elem         string
	permit       int32
	expectedRows int64
}

func (t *KdvhTestCase) mockConfig() (*port.Config, *cache.Cache) {
	return &port.Config{
			Tables:     []string{t.table},
			Stations:   []string{fmt.Sprint(t.station)},
			Elements:   []string{t.elem},
			Path:       "./files",
			Sep:        ";",
			MaxWorkers: 1,
		},
		&cache.Cache{
			Elements: stinfosys.ElemMap{
				{ElemCode: t.elem, TableName: t.table}: {
					Fromtime: time.Date(2001, 7, 1, 9, 0, 0, 0, time.UTC),
					IsScalar: true,
				},
			},
			Permits: stinfosys.PermitMaps{
				StationPermits: stinfosys.StationPermitMap{
					t.station: t.permit,
				},
			},
		}
}

func TestImportKDVH(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	pool, err := pgxpool.New(context.TODO(), LARD_STRING)
	if err != nil {
		t.Log("Could not connect to Lard:", err)
	}
	defer pool.Close()

	testCases := []KdvhTestCase{
		{table: "T_MDATA", station: 12345, elem: "TA", permit: 0, expectedRows: 0},    // restricted TS
		{table: "T_MDATA", station: 12345, elem: "TA", permit: 1, expectedRows: 2644}, // open TS
	}

	kdvh := db.Init()

	// TODO: test does not fail if flags are not inserted
	// TODO: bar does not work well with log print outs
	for _, c := range testCases {
		config, cache := c.mockConfig()

		table, ok := kdvh.Tables[c.table]
		if !ok {
			t.Fatal("Table does not exist in database")
		}

		insertedRows := port.ImportTable(table, cache, pool, config)
		if insertedRows != c.expectedRows {
			t.Fail()
		}
	}
}
