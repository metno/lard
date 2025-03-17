package lard

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Pools struct {
	Open       *pgxpool.Pool
	Restricted *pgxpool.Pool
}

func (p *Pools) AsSlice() []*pgxpool.Pool {
	return []*pgxpool.Pool{p.Open, p.Restricted}
}

func NewLardPool(ctx context.Context) *Pools {
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
	return &Pools{Open: openPool}
}

func (pool *Pools) Close() {
	pool.Open.Close()
	pool.Restricted.Close()
}
