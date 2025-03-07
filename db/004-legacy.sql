CREATE SCHEMA IF NOT EXISTS legacy;

-- Table containing values (possibly corrected) and quality information
-- for observations coming through kvalobs (either from old databases or from the Kafka queue)
CREATE TABLE IF NOT EXISTS legacy.data (
    timeseries INT8 NOT NULL REFERENCES public.timeseries,
    obstime TIMESTAMPTZ NOT NULL,
    -- `original` column is kept in public.(nonscalar_)data table
    -- `corrected` is equal to `original` if the observation passed QC
    -- TODO: should this be NOT NULL?
    corrected FLOAT8 NULL,
    -- quality code of the original observation (derived from useinfo)
    quality_code INT4 NULL,
    controlinfo TEXT NULL,
    useinfo TEXT NULL,
    cfailed TEXT NULL
    CONSTRAINT unique_legacy_data_timeseries_obstime UNIQUE (timeseries, obstime)
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS legacy_data_timestamp_index ON legacy.data (obstime);
CREATE INDEX IF NOT EXISTS legacy_data_timeseries_index ON legacy.data USING HASH (timeseries);
