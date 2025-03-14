package lard

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool struct {
	Open       *pgxpool.Pool
	Restricted *pgxpool.Pool
}

func (p *Pool) GetSlice() []*pgxpool.Pool {
	return []*pgxpool.Pool{p.Open, p.Restricted}
}

func NewLardPool(ctx context.Context) *Pool {
	openPool, err := pgxpool.New(ctx, os.Getenv(LARD_OPEN_ENV_VAR))
	if err != nil {
		fmt.Println("Could not connect to Lard")
		os.Exit(1)
	}

	// restrictedPool, err := pgxpool.New(ctx, os.Getenv(LARD_OPEN_ENV_VAR))
	// if err != nil {
	// 	fmt.Println("Could not connect to Lard")
	// 	os.Exit(1)
	// }

	// return &Pool{Open: openPool, Restricted: restrictedPool}
	return &Pool{Open: openPool}
}

func (pool *Pool) Close() {
	pool.Open.Close()
	pool.Restricted.Close()
}
