package lard

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func DropIndices(pool *pgxpool.Pool) {
	slog.Info("Dropping table indices...")

	file, err := os.ReadFile("../db/drop_indices.sql")
	if err != nil {
		panic(err.Error())
	}

	_, err = pool.Exec(context.Background(), string(file))
	if err != nil {
		panic(err.Error())
	}
	slog.Info("Finished dropping indices!")
}

func CreateIndices(pool *pgxpool.Pool) {
	slog.Info("Creating table indices...")

	files := []string{"../db/create_indices.sql"}
	for _, filename := range files {
		file, err := os.ReadFile(filename)
		if err != nil {
			panic(err.Error())
		}

		_, err = pool.Exec(context.Background(), string(file))
		if err != nil {
			panic(err.Error())
		}
	}
	slog.Info("Finished creating indices!")
}
