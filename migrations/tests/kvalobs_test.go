package tests

import (
	"context"
	"path/filepath"
	"testing"

	kvalobs "migrate/kvalobs/db"
	port "migrate/kvalobs/import"
	"migrate/lard"
	"migrate/stinfosys"
	"migrate/utils"
)

const DUMPS_PATH string = "./files"

type KvalobsTestCase struct {
	test           string
	table          string
	path           string
	station        int32
	paramid        int32
	typeid         int32
	sensor         *int32
	level          *int32
	permit         int32
	skipRestricted bool
	expectedRows   int64
}

func (t *KvalobsTestCase) mockConfig() (*port.Config, *port.Cache) {
	return &port.Config{
			BaseConfig: kvalobs.BaseConfig{
				Path:     t.path,
				Stations: []int32{t.station},
				Test:     true,
			},
			MaxWorkers:     1,
			SkipRestricted: t.skipRestricted,
		},
		&port.Cache{
			Permits: stinfosys.PermitMaps{
				StationPermits: stinfosys.StationPermitMap{
					t.station: t.permit,
				},
			},
		}
}

func TestImportDataKvalobs(t *testing.T) {
	utils.InitLogger()

	pools := lard.NewLardPool(context.TODO())
	defer pools.Close()

	cases := []KvalobsTestCase{
		{
			test:         "open histkvalobs data",
			table:        kvalobs.DataTableName,
			path:         "files/histkvalobs/from_2024-01-01_to_2024-02-01",
			station:      18700,
			paramid:      313,
			permit:       1,
			expectedRows: 39,
		},
		{
			test:           "skip restricted histkvalobs data",
			table:          kvalobs.DataTableName,
			path:           "files/histkvalobs/from_2024-01-01_to_2024-02-01",
			station:        18700,
			paramid:        313,
			permit:         2, // restricted
			skipRestricted: true,
			expectedRows:   0, // skipped
		},
		{
			test:         "open kvalobs text_data",
			table:        kvalobs.TextTableName,
			path:         "files/kvalobs/from_2024-01-01_to_2024-02-01",
			station:      18700,
			permit:       1,
			expectedRows: 182,
		},
	}

	for _, c := range cases {
		config, cache := c.mockConfig()
		table := port.NewTable(c.table)
		path := filepath.Join(config.Path, c.table)

		t.Log(c.test)
		insertedRows, err := table.Import(path, cache, pools, config)

		switch {
		case err != nil:
			t.Fatal(err)
		case insertedRows != c.expectedRows:
			t.Fail()
		}
	}
}
