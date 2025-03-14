package lard

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

func DropIndices(pool *Pool) {
	fmt.Println(time.Now().Format(time.RFC3339), "Dropping table indices...")

	file, err := os.ReadFile("../db/drop_indices.sql")
	if err != nil {
		fmt.Println(err)
		return
	}

	group := errgroup.Group{}

	pools := pool.GetSlice()
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

type Schema struct {
	name  string
	table string
}

func measureExec(ctx context.Context, query string, pool *pgxpool.Pool) error {
	start := time.Now().UTC()
	if _, err := pool.Exec(ctx, query); err != nil {
		fmt.Println(err)
		return err
	}
	end := time.Now().UTC()

	duration := end.Sub(start)
	fmt.Printf("query '%s' took %s\n", query, duration)
	return nil
}

func CreateIndices(pool *Pool) {
	fmt.Println(time.Now().Format(time.RFC3339), "Creating table indices...")

	ctx := context.Background()

	group := errgroup.Group{}
	// pools := pool.GetSlice()
	schemas := []Schema{{"public", "data"}, {"public", "nonscalar_data"}, {"legacy", "data"}}
	p := pool.Open

	// for _, p := range pools {
	if _, err := p.Exec(ctx, "SET maintenance_work_mem TO '2 GB'"); err != nil {
		fmt.Println(err)
	}
	if _, err := p.Exec(ctx, "SET max_parallel_maintenance_workers TO 8"); err != nil {
		fmt.Println(err)
	}

	for _, s := range schemas {
		group.Go(func() error {
			query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_timestamp_index ON %s.%s (obstime)", s.table, s.name, s.table)
			return measureExec(ctx, query, p)
		})
		group.Go(func() error {
			query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_timeseries_index ON %s.%s USING HASH (timeseries)", s.table, s.name, s.table)
			return measureExec(ctx, query, p)
		})
		group.Go(func() error {
			query := fmt.Sprintf("ALTER TABLE %s.%s ADD CONSTRAINT unique_%s_timeseries_obstime UNIQUE (timeseries, obstime)", s.name, s.table, s.table)
			return measureExec(ctx, query, p)
		})
	}
	// }

	if err := group.Wait(); err == nil {
		fmt.Println(time.Now().Format(time.RFC3339), "Finished creating indices!")
	}

	// TODO: maybe we should keep it at 2 GB? Our ingestor doesn't use that much memory
	// and this setting is only used for index creation and vacuuming
	// It might be worth also chaging work_mem (albeit it's a bit more dangerous since we need to figure out
	// what our average/max query load looks like)
	// for _, p := range pools {
	if _, err := p.Exec(ctx, "RESET maintenance_work_mem"); err != nil {
		fmt.Println(err)
	}
	if _, err := p.Exec(ctx, "RESET max_parallel_maintenance_workers"); err != nil {
		fmt.Println(err)
	}
	// }
}
