package lard

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Struct holding dumped CSV data formatted as [][]any for ease of use with pgx.CopyFromRows
type ParsedCsv struct {
	Data [][]any
}

func NewParsedCsv(capacity int) *ParsedCsv {
	return &ParsedCsv{
		Data: make([][]any, 0, capacity),
	}
}

func (p *ParsedCsv) Append(obs []any) {
	p.Data = append(p.Data, obs)
}

// Updates the fromtime of a given timeseries based on the first observation in parsedCsv
func (p *ParsedCsv) UpdateFromtime(pool *pgxpool.Pool) error {
	if len(p.Data) == 0 {
		return nil
	}

	tsid := p.Data[0][0].(int64)
	from := p.Data[0][1].(time.Time)

	_, err := pool.Exec(
		context.TODO(),
		`UPDATE timeseries SET
			fromtime = LEAST($1, fromtime)
			WHERE id = $2`,
		from, tsid,
	)

	return err
}

// Inserts the parsed slices in LARD using postgresql COPY FROM
func (p *ParsedCsv) Insert(pool *pgxpool.Pool) (int64, error) {
	if len(p.Data) == 0 {
		return 0, nil
	}

	// TODO: meeeeh
	// TextObs has only three fields
	nColumns := len(p.Data[0])
	if nColumns == NONSCALAR_DATA_COLUMNS {
		return pool.CopyFrom(
			context.TODO(),
			pgx.Identifier{"public", "nonscalar_data"},
			[]string{"timeseries", "obstime", "obsvalue"},
			pgx.CopyFromRows(p.Data),
		)
	}

	return pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"legacy", "data"},
		[]string{
			"timeseries",
			"obstime",
			"original",
			"corrected",
			"quality_code",
			"controlinfo",
			"useinfo",
			"cfailed",
		},
		pgx.CopyFromRows(p.Data),
	)
}
