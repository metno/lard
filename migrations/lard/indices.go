package lard

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
)

func DropIndices(conn *pgx.Conn) {
	log.Info().Msg("Dropping table indices...")

	file, err := os.ReadFile("../db/drop_indices.sql")
	if err != nil {
		log.Error().Err(err).Msg("")
		return
	}

	_, err = conn.Exec(context.Background(), string(file))
	if err != nil {
		log.Error().Err(err).Msg("")
		return
	}

	log.Info().Msg("Finished dropping indices!")
}

func CreateIndices(conn *pgx.Conn) {
	log.Info().Msg("Creating table indices...")

	file, err := os.ReadFile("../db/create_indices.sql")
	if err != nil {
		log.Error().Err(err).Msg("")
		return
	}

	_, err = conn.Exec(context.Background(), string(file))
	if err != nil {
		log.Error().Err(err).Msg("")
		return
	}

	log.Info().Msg("Finished creating indices!")
}
