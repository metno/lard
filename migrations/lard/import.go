package lard

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Single parsed observation
type ParsedObs struct {
	Data   *DataObs
	Text   *TextObs
	Legacy *LegacyData
}

// Struct holding dumped CSV data
type ParsedCsv struct {
	Data   [][]any
	Text   [][]any
	Legacy [][]any
}

func NewParsedCsv(capacity int) *ParsedCsv {
	return &ParsedCsv{
		Data:   make([][]any, 0, capacity),
		Text:   make([][]any, 0, capacity),
		Legacy: make([][]any, 0, capacity),
	}
}

func (p *ParsedCsv) Append(obs *ParsedObs) {
	if obs.Data != nil {
		p.Data = append(p.Data, obs.Data.ToRow())
	}
	if obs.Text != nil {
		p.Text = append(p.Text, obs.Text.ToRow())
	}
	if obs.Legacy != nil {
		p.Legacy = append(p.Legacy, obs.Legacy.ToRow())
	}
}

// Inserts the parsed slices in LARD using postgresql COPY FROM
func (parsed *ParsedCsv) Insert(pool *pgxpool.Pool) (int64, error) {
	dataCount, err := parsed.insertData(pool)
	if err != nil {
		return 0, err
	}

	textCount, err := parsed.insertTextData(pool)
	if err != nil {
		return 0, err
	}

	// _, err = parsed.insertLegacyFlags(pool)
	// if err != nil {
	// 	return 0, err
	// }

	_, err = parsed.insertLegacyData(pool)
	if err != nil {
		return 0, err
	}

	// Only returning data and text rows, legacy flags and legacy data simply duplicate those counts
	count := dataCount + textCount
	return count, nil
}

func (p *ParsedCsv) insertData(pool *pgxpool.Pool) (int64, error) {
	if len(p.Data) == 0 {
		return 0, nil
	}
	return pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromRows(p.Data),
	)
}

func (p *ParsedCsv) insertTextData(pool *pgxpool.Pool) (int64, error) {
	if len(p.Text) == 0 {
		return 0, nil
	}
	return pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "nonscalar_data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromRows(p.Text),
	)
}

func (p *ParsedCsv) insertLegacyData(pool *pgxpool.Pool) (int64, error) {
	if len(p.Legacy) == 0 {
		return 0, nil
	}
	return pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"legacy", "data"},
		[]string{"timeseries", "obstime", "corrected", "quality_code", "controlinfo", "useinfo", "cfailed"},
		pgx.CopyFromRows(p.Legacy),
	)
}

// func (p *ParsedCsv) insertLegacyFlags(pool *pgxpool.Pool) (int64, error) {
// 	if len(p.Flag) == 0 {
// 		return 0, nil
// 	}
//
// 	return pool.CopyFrom(
// 		context.TODO(),
// 		pgx.Identifier{"legacy", "flags"},
// 		[]string{"timeseries", "obstime"},
// 		pgx.CopyFromRows(p.Flag),
// 	)
// }
