CREATE SCHEMA IF NOT EXISTS legacy;

-- Table containing values (possibly corrected) and quality information
-- for observations coming through kvalobs (either from old databases or from the Kafka queue)
CREATE TABLE IF NOT EXISTS legacy.data (
    timeseries INT8 REFERENCES public.timeseries,
    obstime TIMESTAMPTZ,
    original FLOAT8 NULL,
    corrected FLOAT8 NULL,
    -- quality code of the original observation (derived from useinfo)
    quality_code INT4 NULL,
    controlinfo TEXT NULL,
    useinfo TEXT NULL,
    cfailed TEXT NULL,
   PRIMARY KEY (timeseries, obstime)
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS data_timestamp_index ON legacy.data (obstime);
