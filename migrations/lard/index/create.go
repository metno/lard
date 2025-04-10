package index

import (
	"context"
	"errors"
	"fmt"
	"migrate/lard"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

// Struct for queries from pg_tables
type PgTable struct {
	Schemaname string `db:"schemaname"`
	Tablename  string `db:"tablename"`
}

func measureExec(query string, ctx context.Context, pool *pgxpool.Pool) error {
	start := time.Now()
	if _, err := pool.Exec(ctx, query); err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("Query '%s' took %s\n", query, time.Since(start))
	return nil
}

func (p *PgTable) createIndices(ctx context.Context, pool *pgxpool.Pool) error {
	constrErr := measureExec(
		fmt.Sprintf(
			"ALTER TABLE %s.%s ADD CONSTRAINT unique_%s_timeseries_obstime UNIQUE (timeseries, obstime)",
			p.Schemaname, p.Tablename, p.Tablename,
		),
		ctx,
		pool,
	)

	btreeErr := measureExec(
		fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS %s_timestamp_index ON %s.%s (obstime)",
			p.Tablename, p.Schemaname, p.Tablename,
		),
		ctx,
		pool,
	)

	hashErr := measureExec(
		fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS %s_timeseries_index ON %s.%s USING HASH (timeseries)",
			p.Tablename, p.Schemaname, p.Tablename,
		),
		ctx,
		pool,
	)

	return errors.Join(constrErr, btreeErr, hashErr)
}

func findPartitions(ctx context.Context, pool *pgxpool.Pool) ([]PgTable, error) {
	rows, err := pool.Query(ctx, "select schemaname, tablename from pg_tables where tablename like '%_to_y%'")
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	partitions, err := pgx.CollectRows(rows, pgx.RowToStructByName[PgTable])
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return partitions, nil
}

func CreateIndices(database string) {
	fmt.Println(time.Now().Format(time.RFC3339), "Creating table indices...")

	ctx := context.Background()
	pools := lard.NewLardPool(ctx)

	group := errgroup.Group{}

	schemas := []PgTable{{"public", "data"}, {"public", "nonscalar_data"}, {"legacy", "data"}}

	for name, pool := range pools.AsMap() {
		if database != "" && name != database {
			continue
		}

		partitions, err := findPartitions(ctx, pool)
		if err != nil {
			continue
		}

		// First create indices for the individual partitions
		for _, p := range partitions {
			group.Go(func() error {
				return p.createIndices(ctx, pool)
			})
		}

		err = group.Wait()
		if err != nil {
			continue
		}

		// Create indices on parent tables
		for _, s := range schemas {
			group.Go(func() error {
				return s.createIndices(ctx, pool)
			})
		}

		if err := group.Wait(); err == nil {
			fmt.Printf("%s: Finished creating indices for %s database\n", time.Now().Format(time.RFC3339), name)
		}
	}
}
