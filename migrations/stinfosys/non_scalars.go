package stinfosys

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
)

func GetNonScalars(conn *pgx.Conn) []int32 {
	rows, err := conn.Query(context.TODO(), "SELECT paramid FROM param WHERE scalar = false ORDER BY paramid")
	if err != nil {
		log.Error().Err(err).Msg("")
		os.Exit(1)
	}
	nonscalars, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		log.Error().Err(err).Msg("")
		os.Exit(1)
	}
	return nonscalars
}
