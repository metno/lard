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

type Table struct {
	Schema string `db:"schemaname"`
	Name   string `db:"tablename"`
}

func (t *Table) addPkey(ctx context.Context, pool *pgxpool.Pool) error {
	query := fmt.Sprintf(
		"ALTER TABLE %s.%s ADD PRIMARY KEY (timeseries, obstime)",
		t.Schema, t.Name,
	)

	_, err := pool.Exec(ctx, query)
	return err
}

func (t *Table) createTimestampIndex(ctx context.Context, pool *pgxpool.Pool) error {
	query := fmt.Sprintf(
		"CREATE INDEX %[1]s_timestamp_index ON ONLY %[2]s.%[1]s USING btree (obstime)",
		t.Name, t.Schema,
	)

	_, err := pool.Exec(ctx, query)
	return err

}

func (t *Table) createFkeyConstraint(ctx context.Context, pool *pgxpool.Pool) error {
	query := fmt.Sprintf(
		`ALTER TABLE %[1]s.%[2]s ADD CONSTRAINT %[2]s_timeseries_fkey
			FOREIGN KEY (timeseries) REFERENCES public.timeseries`,
		t.Schema,
		t.Name,
	)

	_, err := pool.Exec(ctx, query)
	return err
}

func findPartitions(ctx context.Context, pool *pgxpool.Pool, opts SelectOptions) ([]Table, error) {
	rows, err := pool.Query(ctx,
		`SELECT schemaname, tablename FROM pg_tables
			WHERE ($1 IS TRUE AND (schemaname = 'public' AND tablename LIKE 'nonscalar%y%'))
			OR    ($2 IS TRUE AND (schemaname = 'legacy' AND tablename LIKE 'data%y%'))`,
		opts.text, opts.data,
	)
	if err != nil {
		fmt.Println("error querying pg_tables: ", err)
		return nil, err
	}

	partitions, err := pgx.CollectRows(rows, pgx.RowToStructByName[Table])
	if err != nil {
		fmt.Println("error collecting rows: ", err)
		return nil, err
	}

	return partitions, nil
}

func (t *Table) createIndices(ctx context.Context, pool *pgxpool.Pool) error {
	if err := t.addPkey(ctx, pool); err != nil {
		fmt.Println("error adding pkey: ", err)
		return err
	}
	fmt.Printf("Added pkey on %s.%s\n", t.Schema, t.Name)

	if err := t.createFkeyConstraint(ctx, pool); err != nil {
		fmt.Println("error creating fkey constraint: ", err)
		return err
	}
	fmt.Printf("Created fkey constraint on %s.%s\n", t.Schema, t.Name)

	if err := t.createTimestampIndex(ctx, pool); err != nil {
		fmt.Println("error creating obstime index: ", err)
		return err
	}
	fmt.Printf("Created obstime index on %s.%s\n", t.Schema, t.Name)

	return nil
}

// Attach each partition timestamp index to the parent table in order to render the parent table index valid
func (t *Table) attachPartitions(ctx context.Context, partitions []Table, pool *pgxpool.Pool) error {
	for _, p := range partitions {
		if p.Schema != t.Schema {
			continue
		}
		query := fmt.Sprintf(
			"ALTER INDEX %s.%s_timestamp_index ATTACH PARTITION %s.%s_timestamp_index",
			t.Schema, t.Name, p.Schema, p.Name,
		)
		_, err := pool.Exec(ctx, query)
		if err != nil {
			return err
		}
	}
	return nil
}

func CreateIndices(database string, opts SelectOptions) {
	fmt.Println(time.Now().Format(time.RFC3339), "Creating table indices...")
	ctx := context.Background()

	runtimeParams := map[string]string{
		// TODO: maybe we should keep it at 2 GB? Our ingestor doesn't use that much memory
		// and this setting is only used for index creation and vacuuming
		// It might be worth also chaging work_mem (albeit it's a bit more dangerous since we need to figure out
		// what our average/max query load looks like)
		"maintenance_work_mem":             "2 GB",
		"max_parallel_maintenance_workers": "8",
	}

	pools := lard.NewLardPoolWithParams(ctx, runtimeParams)
	group := errgroup.Group{}

	for name, pool := range pools.AsMap() {
		if database != "" && name != database {
			continue
		}

		partitions, err := findPartitions(ctx, pool, opts)
		if err != nil {
			continue
		}

		start := time.Now()
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

		// Create indices on parent legacydata table
		if opts.data {
			t := Table{"legacy", "data"}
			group.Go(func() error {
				err := t.createIndices(ctx, pool)
				if err != nil {
					return err
				}
				return t.attachPartitions(ctx, partitions, pool)
			})
		}

		// Create indices on parent public.nonscalar_data table
		if opts.text {
			t := Table{"public", "nonscalar_data"}
			group.Go(func() error {
				err := t.createIndices(ctx, pool)
				if err != nil {
					return err
				}
				return t.attachPartitions(ctx, partitions, pool)
			})
		}

		group.Wait()
		fmt.Printf("Database %q: %s\n", name, time.Since(start))
	}
}
