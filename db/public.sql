DO $$ 
BEGIN
    IF (SELECT NOT EXISTS (select from pg_type where typname = 'location')) THEN
    CREATE TYPE location AS (
        lat REAL,
        lon REAL,
        hamsl REAL,
        hag REAL
    );
END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.timeseries (
    id SERIAL PRIMARY KEY,
    fromtime TIMESTAMPTZ NULL,
    totime TIMESTAMPTZ NULL,
    loc location NULL, 
    deactivated BOOL NULL
);

CREATE TABLE IF NOT EXISTS public.data (
    timeseries INT4 NOT NULL,
    obstime TIMESTAMPTZ NOT NULL,
    obsvalue REAL,
    CONSTRAINT unique_data_timeseries_obstime UNIQUE (timeseries, obstime),
    CONSTRAINT fk_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS data_timestamp_index ON public.data (obstime);
CREATE INDEX IF NOT EXISTS data_timeseries_index ON public.data USING HASH (timeseries);
