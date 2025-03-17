package index

import (
	"context"
	"errors"
	"fmt"
	"migrate/lard"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"golang.org/x/sync/errgroup"
)

type Config struct {
	Action string `arg:"positional" help:"Valid choices: [\"drop\", \"create\"]"`
}

func (config *Config) Execute() error {
	err := godotenv.Load()
	if err != nil {
		return err
	}

	pools := lard.NewLardPool(context.Background())
	defer pools.Close()

	switch config.Action {
	case "drop":
		DropIndices(pools)
	case "create":
		CreateIndices(pools)
	default:
		return fmt.Errorf("Invalid argumnent '%s'", config.Action)
	}
	return nil
}

// Struct for queries from pg_tables
type PgTable struct {
	Schemaname string `db:"schemaname"`
	Tablename  string `db:"tablename"`
}

func DropIndices(pool *lard.Pools) {
	fmt.Println(time.Now().Format(time.RFC3339), "Dropping table indices...")

	file, err := os.ReadFile("../db/drop_indices.sql")
	if err != nil {
		fmt.Println(err)
		return
	}

	group := errgroup.Group{}

	pools := pool.AsSlice()
	for _, p := range pools {
		group.Go(func() error {
			_, err := p.Exec(context.Background(), string(file))
			if err != nil {
				fmt.Println(err)
				return err
			}
			return nil
		})
	}

	if err := group.Wait(); err == nil {
		fmt.Println(time.Now().Format(time.RFC3339), "Finished dropping indices!")
	}
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
	rows, err := pool.Query(ctx, "select * from pg_tables where tablename like '%_to_y%'")
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

func CreateIndices(pools *lard.Pools) {
	fmt.Println(time.Now().Format(time.RFC3339), "Creating table indices...")

	ctx := context.Background()
	group := errgroup.Group{}

	schemas := []PgTable{{"public", "data"}, {"public", "nonscalar_data"}, {"legacy", "data"}}

	for _, pool := range pools.AsSlice() {
		if _, err := pool.Exec(ctx, "SET maintenance_work_mem TO '2 GB'"); err != nil {
			fmt.Println(err)
		}

		if _, err := pool.Exec(ctx, "SET max_parallel_maintenance_workers TO 8"); err != nil {
			fmt.Println(err)
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
			fmt.Println(time.Now().Format(time.RFC3339), "Finished creating indices!")
		}

		// TODO: maybe we should keep it at 2 GB? Our ingestor doesn't use that much memory
		// and this setting is only used for index creation and vacuuming
		// It might be worth also chaging work_mem (albeit it's a bit more dangerous since we need to figure out
		// what our average/max query load looks like)
		// for _, p := range pools {
		if _, err := pool.Exec(ctx, "RESET maintenance_work_mem"); err != nil {
			fmt.Println(err)
		}
		if _, err := pool.Exec(ctx, "RESET max_parallel_maintenance_workers"); err != nil {
			fmt.Println(err)
		}
	}
}
