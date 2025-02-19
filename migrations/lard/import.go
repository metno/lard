package lard

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Single parsed observation
type ParsedObs struct {
	Data   *DataObs
	Text   *TextObs
	Legacy *LegacyData
	Flag   *LegacyFlag
}

type ParsedCsv struct {
	Data   [][]any
	Text   [][]any
	Legacy [][]any
	Flag   [][]any
}

func InitParsedCsv(capacity int) *ParsedCsv {
	return &ParsedCsv{
		Data:   make([][]any, 0, capacity),
		Text:   make([][]any, 0, capacity),
		Legacy: make([][]any, 0, capacity),
		Flag:   make([][]any, 0, capacity),
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
	if obs.Flag != nil {
		p.Flag = append(p.Flag, obs.Flag.ToRow())
	}
}

func (parsed *ParsedCsv) Insert(pool *pgxpool.Pool, logStr string) (int64, error) {
	data, err := parsed.InsertData(pool, logStr)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}
	text, err := parsed.InsertTextData(pool, logStr)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}
	legacy, err := parsed.InsertLegacyFlags(pool, logStr)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}
	flags, err := parsed.InsertLegacyData(pool, logStr)
	if err != nil {
		slog.Error(logStr + err.Error())
		return 0, err
	}
	return data + text + legacy + flags, nil
}

func (p *ParsedCsv) InsertData(pool *pgxpool.Pool, logStr string) (int64, error) {
	size := len(p.Data)
	if size == 0 {
		return 0, nil
	}
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "data"},
		[]string{"timeseries", "obstime", "obsvalue", "qc_usable"},
		pgx.CopyFromRows(p.Data),
	)
	if err != nil {
		return count, err
	}

	logStr += fmt.Sprintf("%v/%v data rows inserted", count, size)
	if int(count) != size {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return count, nil
}

func (p *ParsedCsv) InsertTextData(pool *pgxpool.Pool, logStr string) (int64, error) {
	size := len(p.Text)
	if size == 0 {
		return 0, nil
	}
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "nonscalar_data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromRows(p.Text),
	)
	if err != nil {
		return count, err
	}

	logStr += fmt.Sprintf("%v/%v text rows inserted", count, size)
	if int(count) != size {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return count, nil
}

func (p *ParsedCsv) InsertLegacyData(pool *pgxpool.Pool, logStr string) (int64, error) {
	size := len(p.Legacy)
	if size == 0 {
		return 0, nil
	}
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "legacy_data"},
		[]string{"timeseries", "obstime", "corrected", "quality"},
		pgx.CopyFromRows(p.Legacy),
	)
	if err != nil {
		return count, err
	}

	logStr += fmt.Sprintf("%v/%v legacy rows inserted", count, size)
	if int(count) != size {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return count, nil
}

// TODO: maybe this should also return a insert count for testing purposes
func (p *ParsedCsv) InsertLegacyFlags(pool *pgxpool.Pool, logStr string) (int64, error) {
	size := len(p.Flag)
	if size == 0 {
		return 0, nil
	}

	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"flags", "legacy"},
		[]string{"timeseries", "obstime", "controlinfo", "useinfo", "cfailed"},
		pgx.CopyFromRows(p.Flag),
	)
	if err != nil {
		return count, err
	}

	logStr += fmt.Sprintf("%v/%v flag rows inserted", count, size)
	if int(count) != size {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return count, nil
}
