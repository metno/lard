package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	kdvh "migrate/kdvh/db"
	port "migrate/kdvh/import"
	"migrate/lard"
	"migrate/stinfosys"
	"migrate/utils"
)

type KdvhTestCase struct {
	table        string
	station      int32
	elem         string
	permit       int32
	expectedRows int64
}

func (t *KdvhTestCase) mockConfig() (*port.Config, *port.Cache) {
	return &port.Config{
			BaseConfig: kdvh.BaseConfig{
				Tables:   []string{t.table},
				Stations: []string{fmt.Sprint(t.station)},
				Elements: []string{t.elem},
				Path:     "./files",
			},
			Sep:        ";",
			MaxWorkers: 1,
		},
		&port.Cache{
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
	utils.InitLogger()

	pools := lard.NewLardPool(context.TODO())
	defer pools.Close()

	testCases := []KdvhTestCase{
		{table: "T_MDATA", station: 12345, elem: "TA", permit: 0, expectedRows: 2644}, // restricted TS
		{table: "T_MDATA", station: 12345, elem: "TA", permit: 1, expectedRows: 2644}, // open TS
	}

	kdvh := port.InitImportTables()

	// TODO: test does not fail if flags are not inserted
	// TODO: bar does not work well with log print outs
	for _, c := range testCases {
		config, cache := c.mockConfig()

		for _, table := range kdvh {
			if c.table != table.TableName {
				continue
			}
			insertedRows := table.Import(cache, pools, config)
			if insertedRows != c.expectedRows {
				t.Fail()
			}

		}

	}
}
