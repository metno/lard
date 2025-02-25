package lard

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

// TODO: use fmt here!
func DropIndices(conn *pgx.Conn) {
	fmt.Println(time.Now().Format(time.RFC3339), "Dropping table indices...")

	file, err := os.ReadFile("../db/drop_indices.sql")
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = conn.Exec(context.Background(), string(file))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(time.Now().Format(time.RFC3339), "Finished dropping indices!")
}

func CreateIndices(conn *pgx.Conn) {
	fmt.Println(time.Now().Format(time.RFC3339), "Creating table indices...")

	file, err := os.ReadFile("../db/create_indices.sql")
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = conn.Exec(context.Background(), string(file))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(time.Now().Format(time.RFC3339), "Finished creating indices!")
}
