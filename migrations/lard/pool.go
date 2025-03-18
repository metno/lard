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

func (p *Pools) AsMap() map[string]*pgxpool.Pool {
	return map[string]*pgxpool.Pool{"lard": p.Open, "lard_restricted": p.Restricted}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// Return a pool with the given runtime parameters
func NewLardPoolWithParams(ctx context.Context, params map[string]string) *Pools {
	openConfig, err := pgxpool.ParseConfig(getEnv(LARD_OPEN_ENV_VAR, TEST_CONN_STRING_OPEN))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	restrictedConfig, err := pgxpool.ParseConfig(getEnv(LARD_RESTRICTED_ENV_VAR, TEST_CONN_STRING_RESTRICTED))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	openConfig.ConnConfig.RuntimeParams = params
	restrictedConfig.ConnConfig.RuntimeParams = params

	openPool, err := pgxpool.NewWithConfig(ctx, openConfig)
	if err != nil {
		fmt.Println("Could not connect to Lard")
		os.Exit(1)
	}

	restrictedPool, err := pgxpool.NewWithConfig(ctx, restrictedConfig)
	if err != nil {
		fmt.Println("Could not connect to Lard")
		os.Exit(1)
	}

	return &Pools{Open: openPool, Restricted: restrictedPool}

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
