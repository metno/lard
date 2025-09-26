DO $$
BEGIN
    IF (SELECT NOT EXISTS (select from pg_type where typname = 'location')) THEN
    CREATE TYPE location AS (
        lat FLOAT8,
        lon FLOAT8,
        hamsl FLOAT8,
        hag FLOAT8
    );
END IF;
END $$;

DO $$ 
BEGIN
    IF (SELECT NOT EXISTS (select from pg_type where typname = 'obs')) THEN
    CREATE TYPE obs AS (
        obstime TIMESTAMPTZ,
        obsvalue FLOAT8
    );
END IF;
END $$;

DO $$
BEGIN
    IF (SELECT NOT EXISTS (select from pg_type where typname = 'windobs')) THEN
    -- Both speed and direction are `obsvalue`s from the data table.
    -- They are grouped in this type as convenience for calculation of the windrose report
    CREATE TYPE windobs AS (
        speed FLOAT8,
        direction FLOAT8
    );
END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.timeseries (
    id SERIAL8 PRIMARY KEY,
    fromtime TIMESTAMPTZ NULL,
    totime TIMESTAMPTZ NULL,
    loc location NULL, 
    -- code from stinfosys indicating rules for sharing data
    -- TODO: fill out remaining values and descriptions here
    -- 1 - Open, all entries in the non restricted db should be 1
    -- NULL - No permit found in stinfosys, assume closed. Others (I think Vegar and Terje) have
    -- suggested we instead treat this as open, but I (Ingrid) am personally not willing to be
    -- responsible for taking that risk
    permit INT4 NULL,
    deactivated BOOL NULL
);

CREATE TABLE IF NOT EXISTS public.data (
    timeseries INT8,
    obstime TIMESTAMPTZ,
    obsvalue FLOAT8,
    -- This value should not be treated as an absolute assertion of the data's quality but rather
    -- our current knowlege of it. `true` here indicates that the datum has not failed any QC
    -- pipelines (including if none have been run at all). Users that have specific requirements
    -- for what QC has been performed on the data should refer to the information in the
    -- `flags.confident_provenance` table.
    qc_usable BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT fk_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries,
    PRIMARY KEY (timeseries, obstime)
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS data_timestamp_index ON public.data (obstime);

-- TODO: this should be renamed to 'public.text' or 'public.string'
CREATE TABLE IF NOT EXISTS public.nonscalar_data (
    timeseries INT8,
    obstime TIMESTAMPTZ,
    obsvalue TEXT,
    qc_usable BOOLEAN,
    CONSTRAINT fk_nonscalar_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries,
    PRIMARY KEY (timeseries, obstime)
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS nonscalar_data_timestamp_index ON public.nonscalar_data (obstime);
