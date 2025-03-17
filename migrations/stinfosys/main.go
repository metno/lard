package stinfosys

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

const STINFOSYS_ENV_VAR string = "STINFO_CONN_STRING"

func Connect() (*pgx.Conn, context.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, os.Getenv(STINFOSYS_ENV_VAR))
	if err != nil {
		fmt.Println("Could not connect to Stinfosys. Make sure to be connected to the VPN.")
		os.Exit(1)
	}
	return conn, ctx
}
