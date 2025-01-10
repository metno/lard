package db

import (
	"migrate/utils"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Maps to `data` and `text_data` tables in Kvalobs
type Table struct {
	Name       string
	Path       string        // Path of the dumped table
	DumpLabels LabelDumpFunc // Function that dumps labels from the table
	DumpSeries ObsDumpFunc   // Function that dumps observations from the table
	Import     ImportFunc    // Function that parses dumps and ingests observations into LARD
}

func (t *Table) SetPath(path string) {
	t.Path = path
}

// Function used to query labels from kvalobs given an optional timespan
type LabelDumpFunc func(timespan *utils.TimeSpan, db *DB, pool *pgxpool.Pool, maxConn int) ([]*Label, error)

// Function used to query timeseries from kvalobs for a specific label and dump them inside path
type ObsDumpFunc func(label *Label, timespan *utils.TimeSpan, path string, pool *pgxpool.Pool) error

// Lard Import function
type ImportFunc func(tsid int64, label *Label, filename, logStr string, pool *pgxpool.Pool) (int64, error)
