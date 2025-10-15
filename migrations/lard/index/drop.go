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
type Index struct {
	Name   string `db:"indexname"`
	Schema string `db:"schemaname"`
}

type Constraint struct {
	Name   string `db:"constraint_name"`
	Schema string `db:"table_schema"`
	Table  string `db:"table_name"`
}

func findIndices(ctx context.Context, pool *pgxpool.Pool, opts SelectOptions) ([]Index, error) {
	rows, err := pool.Query(
		ctx,
		`SELECT schemaname, indexname fROM pg_indexes
            WHERE ($1 IS TRUE AND (schemaname = 'public' AND tablename LIKE 'nonscalar_data%'))
			   OR ($2 IS TRUE AND (schemaname = 'legacy' AND tablename LIKE 'data%'))`,
		opts.text, opts.data,
	)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	indices, err := pgx.CollectRows(rows, pgx.RowToStructByName[Index])
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return indices, nil
}

func findConstraints(ctx context.Context, pool *pgxpool.Pool, opts SelectOptions) ([]Constraint, error) {
	rows, err := pool.Query(ctx,
		`SELECT constraint_name, table_schema, table_name
		 FROM information_schema.table_constraints
		 WHERE constraint_type in ('FOREIGN KEY', 'PRIMARY KEY', 'UNIQUE')
		 AND (
			($1 IS TRUE AND (table_schema = 'public' and table_name LIKE 'nonscalar_data%')) OR
		    ($2 IS TRUE AND (table_schema = 'legacy' and table_name LIKE 'data%'))
		 )`,
		opts.text, opts.data,
	)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	constraints, err := pgx.CollectRows(rows, pgx.RowToStructByName[Constraint])
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return constraints, nil
}

func DropIndices(database string, opts SelectOptions) {
	pools := lard.NewLardPool(context.Background())
	defer pools.Close()

	fmt.Println(time.Now().Format(time.RFC3339), "Dropping table indices...")

	ctx := context.Background()
	group := errgroup.Group{}

	for name, pool := range pools.AsMap() {
		if database != "" && name != database {
			continue
		}

		contraints, err := findConstraints(ctx, pool, opts)
		if err != nil {
			continue
		}

		for _, c := range contraints {
			group.Go(func() error {
				_, err := pool.Exec(ctx,
					fmt.Sprintf("ALTER TABLE %s.%s DROP CONSTRAINT %s", c.Schema, c.Table, c.Name),
				)
				return err
			})
		}

		if err := group.Wait(); err != nil {
			fmt.Println(err)
			continue
		}

		indices, err := findIndices(ctx, pool, opts)
		if err != nil {
			continue
		}

		for _, idx := range indices {
			group.Go(func() error {
				_, err := pool.Exec(ctx,
					fmt.Sprintf("DROP INDEX IF EXISTS %s.%s", idx.Schema, idx.Name),
				)
				return err
			})
		}

		if err := group.Wait(); err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Printf("%s: Finished dropping indices for %s database!\n", time.Now().Format(time.RFC3339), name)
	}

}
