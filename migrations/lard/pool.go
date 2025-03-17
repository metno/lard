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

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func NewLardPool(ctx context.Context) *Pools {
	openPool, err := pgxpool.New(ctx, getEnv(LARD_OPEN_ENV_VAR, TEST_CONN_STRING_OPEN))
	if err != nil {
		fmt.Println("Could not connect to Lard")
		os.Exit(1)
	}

	restrictedPool, err := pgxpool.New(ctx, getEnv(LARD_RESTRICTED_ENV_VAR, TEST_CONN_STRING_RESTRICTED))
	if err != nil {
		fmt.Println("Could not connect to Lard")
		os.Exit(1)
	}

	return &Pools{Open: openPool, Restricted: restrictedPool}
}

func (pool *Pools) Close() {
	pool.Open.Close()
	pool.Restricted.Close()
}
