package lard

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5"
)

func DropIndices(conn *pgx.Conn) {
	slog.Info("Dropping table indices...")

	file, err := os.ReadFile("../db/drop_indices.sql")
	if err != nil {
		slog.Error(err.Error())
		return
	}

	_, err = conn.Exec(context.Background(), string(file))
	if err != nil {
		slog.Error(err.Error())
		return
	}

	slog.Info("Finished dropping indices!")
}

func CreateIndices(conn *pgx.Conn) {
	slog.Info("Creating table indices...")

	file, err := os.ReadFile("../db/create_indices.sql")
	if err != nil {
		slog.Error(err.Error())
		return
	}

	_, err = conn.Exec(context.Background(), string(file))
	if err != nil {
		slog.Error(err.Error())
		return
	}

	slog.Info("Finished creating indices!")
}
