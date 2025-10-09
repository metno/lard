package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	kdvh "migrate/kdvh/db"
	port "migrate/kdvh/import"
	"migrate/lard"
	"migrate/stinfosys"
	"migrate/utils"
)

type KdvhTestCase struct {
	test         string
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
				Test:     true,
			},
			Sep:        ";",
			MaxWorkers: 1,
		},
		&port.Cache{
			Elements: stinfosys.ElemMap{
				{ElemCode: t.elem, TableName: t.table}: {
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
		{
			test:         "kdvh restricted data",
			table:        "T_MDATA",
			station:      12345,
			elem:         "TA",
			permit:       0,
			expectedRows: 2644,
		},
		{
			test:         "kdvh open data",
			table:        "T_MDATA",
			station:      12345,
			elem:         "TA",
			permit:       1,
			expectedRows: 2644,
		},
	}

	// TODO: test does not fail if flags are not inserted
	// TODO: bar does not work well with log print outs
	for _, c := range testCases {
		config, cache := c.mockConfig()
		table := port.IMPORT_TABLES[c.table]

		t.Log(c.test)

		path := filepath.Join(config.Path, c.table)
		insertedRows := table.Import(path, cache, pools, config)
		if insertedRows != c.expectedRows {
			t.Fail()
		}

	}
}
