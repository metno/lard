SET maintenance_work_mem TO '2 GB'; -- 2 GB x (4 + 1) workers = 10 GB

DO $$
DECLARE
 i text[];
BEGIN
    FOREACH i SLICE 1 IN ARRAY ARRAY[['public', 'data'], ['public', 'nonscalar_data'], ['flags', 'kvdata']]
    LOOP
        EXECUTE format('ALTER TABLE %s.%s SET (parallel_workers = 4)', i[1], i[2]);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %s_timestamp_index ON %s.%s (obstime)', i[2], i[1], i[2]);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %s_timeseries_index ON %s.%s USING HASH (timeseries)', i[2], i[1], i[2]);
        EXECUTE format('ALTER TABLE %s.%s RESET (parallel_workers)', i[1], i[2]);
    END LOOP;
END $$;

-- TODO: maybe we should keep it at 2 GB? Our ingestor doesn't use that much memory
-- and this setting is only used for index creation and vacuuming
-- It might be worth also chaging work_mem (albeit it's a bit more dangerous since we need to figure out
-- what our average/max query load looks like)
RESET maintenance_work_mem; 
