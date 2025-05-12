package tests

import (
	"context"
	"testing"
	"time"

	kvalobs "migrate/kvalobs/db"
	port "migrate/kvalobs/import"
	"migrate/lard"
	"migrate/stinfosys"
	"migrate/utils"
)

const DUMPS_PATH string = "./files"

type KvalobsTestCase struct {
	db             string
	table          string
	station        int32
	paramid        int32
	typeid         int32
	sensor         *int32
	level          *int32
	permit         int32
	skipRestricted bool
	expectedRows   int64
}

func (c *KvalobsTestCase) setSkipRestricted(config *port.Config) {
	if c.skipRestricted {
		config.SkipRestricted = true
		return
	}
	config.SkipRestricted = false
}

func (t *KvalobsTestCase) mockConfig() (*port.Config, *port.Cache) {
	fromtime, _ := time.Parse(time.DateOnly, "1900-01-01")
	return &port.Config{
			BaseConfig: kvalobs.BaseConfig{
				Path:     "files",
				Stations: []int32{t.station},
			},
			SpanDir:    "from_2024-01-01_to_2024-02-01",
			MaxWorkers: 1,
		},
		&port.Cache{
			Meta: map[string]map[port.MetaKey]utils.TimeSpan{
				"kvalobs":     {{Stationid: t.station}: {From: &fromtime}},
				"histkvalobs": {{Stationid: t.station}: {From: &fromtime}},
			},
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
			db:           "histkvalobs",
			table:        "data",
			station:      18700,
			paramid:      313,
			permit:       1,
			expectedRows: 39,
		},
		{
			db:             "histkvalobs",
			table:          "data",
			station:        18700,
			paramid:        313,
			permit:         2, // restricted
			skipRestricted: true,
			expectedRows:   0, // skipped
		},
		{
			db:           "kvalobs",
			table:        "text_data",
			station:      18700,
			permit:       1,
			expectedRows: 182,
		},
	}

	tables := port.InitImportTables()
	for _, c := range cases {
		config, cache := c.mockConfig()

		var table *port.Table
		for _, t := range tables {
			if t.DbName == c.db && t.Name == c.table {
				table = t
				break
			}
		}

		if table == nil {
			t.Fatalf("Test case is invalid: db = %s, table = %s", c.db, c.table)
		}

		c.setSkipRestricted(config)

		insertedRows, err := table.Import(cache, pools, config)
		switch {
		case err != nil:
			t.Fatal(err)
		case insertedRows != c.expectedRows:
			t.Fail()
		}
	}
}
