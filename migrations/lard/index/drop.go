package index

import (
	"context"
	"fmt"
	"migrate/lard"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

// Struct for queries from pg_indexes
type PgIndex struct {
	Schemaname string `db:"schemaname"`
	Indexname  string `db:"indexname"`
}

func findIndices(ctx context.Context, pool *pgxpool.Pool) ([]PgIndex, error) {
	rows, err := pool.Query(
		ctx,
		`SELECT schemaname, indexname fROM pg_indexes
            WHERE schemaname IN ('public', 'legacy')
			AND tablename IN ('data', 'nonscalar_data')
            AND NOT indexdef LIKE '%UNIQUE%'`,
	)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	indices, err := pgx.CollectRows(rows, pgx.RowToStructByName[PgIndex])
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return indices, nil
}

func DropIndices(database string) {
	pools := lard.NewLardPool(context.Background())
	defer pools.Close()

	fmt.Println(time.Now().Format(time.RFC3339), "Dropping table indices...")

	ctx := context.Background()
	group := errgroup.Group{}

	for name, pool := range pools.AsMap() {
		if database != "" && name != database {
			continue
		}

		indices, err := findIndices(ctx, pool)
		if err != nil {
			continue
		}

		for _, idx := range indices {
			group.Go(func() error {
				_, err := pool.Exec(ctx, fmt.Sprintf("DROP INDEX IF EXISTS %s.%s", idx.Schemaname, idx.Indexname))
				return err
			})
		}
		if err := group.Wait(); err == nil {
			fmt.Printf("%s: Finished dropping indices for %s database!\n", time.Now().Format(time.RFC3339), name)
		}
	}

}
