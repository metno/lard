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
    timeseries INT8 NOT NULL,
    obstime TIMESTAMPTZ NOT NULL,
    obsvalue FLOAT8,
    -- This value should not be treated as an absolute assertion of the data's quality but rather
    -- our current knowlege of it. `true` here indicates that the datum has not failed any QC
    -- pipelines (including if none have been run at all). Users that have specific requirements
    -- for what QC has been performed on the data should refer to the information in the
    -- `flags.confident_provenance` table.
    qc_usable BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT unique_data_timeseries_obstime UNIQUE (timeseries, obstime),
    CONSTRAINT fk_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS data_timestamp_index ON public.data (obstime);
CREATE INDEX IF NOT EXISTS data_timeseries_index ON public.data USING HASH (timeseries);


CREATE TABLE IF NOT EXISTS public.nonscalar_data (
    timeseries INT8 NOT NULL,
    obstime TIMESTAMPTZ NOT NULL,
    obsvalue TEXT,
    qc_usable BOOLEAN,
    CONSTRAINT unique_nonscalar_data_timeseries_obstime UNIQUE (timeseries, obstime),
    CONSTRAINT fk_nonscalar_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS nonscalar_data_timestamp_index ON public.nonscalar_data (obstime);
CREATE INDEX IF NOT EXISTS nonscalar_data_timeseries_index ON public.nonscalar_data USING HASH (timeseries);

-- Table containing values (possibly corrected) and quality information
-- for observations coming through kvalobs (either from old databases or from the Kafka queue)
CREATE TABLE IF NOT EXISTS legacy_data (
    timeseries INT8 NOT NULL REFERENCES public.timeseries,
    obstime TIMESTAMPTZ NOT NULL,
    -- original is kept in the (nonscalar_)data table
    -- TODO: should it be kept here instead?
    -- TODO: not sure splitting like this will help query speed?
    corrected FLOAT8 NOT NULL,
    -- quality code of the original observation (derived from useinfo)
    -- TODO: should this be converted to `qc_usable`?
    quality INT4 NULL,
    CONSTRAINT unique_kvdata_timeseries_obstime UNIQUE (timeseries, obstime)
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS legacy_data_timestamp_index ON public.legacy_data (obstime);
CREATE INDEX IF NOT EXISTS legacy_data_timeseries_index ON public.legacy_data USING HASH (timeseries);
