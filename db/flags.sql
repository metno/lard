CREATE SCHEMA IF NOT EXISTS flags;

-- TODO: should this also have a column for qc_time or some such?
CREATE TABLE IF NOT EXISTS flags.confident_provenance (
    timeseries INT8 NOT NULL,
    obstime TIMESTAMPTZ NOT NULL,
    pipeline TEXT NOT NULL,
    -- TODO: should this be an enum?
    flag INT4 NOT NULL,
    -- TODO: better name? since this might be applied to flags that aren't fail but also aren't pass?
    fail_condition TEXT NULL,
    CONSTRAINT unique_confident_provenance_timeseries_obstime_pipeline UNIQUE (timeseries, obstime, pipeline),
    CONSTRAINT fk_confident_provenance_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS confident_provenance_timestamp_index ON flags.confident_provenance (obstime);
CREATE INDEX IF NOT EXISTS confident_provenance_timeseries_index ON flags.confident_provenance USING HASH (timeseries);

-- Table containing whole kvalobs flags
CREATE TABLE IF NOT EXISTS flags.legacy (
    timeseries INT8 NOT NULL REFERENCES public.timeseries,
    obstime TIMESTAMPTZ NOT NULL,
    controlinfo TEXT NULL,
    useinfo TEXT NULL,
    cfailed TEXT NULL,
    CONSTRAINT unique_legacy_flags_timeseries_obstime UNIQUE (timeseries, obstime)
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS legacy_flags_timestamp_index ON  flags.legacy (obstime);
CREATE INDEX IF NOT EXISTS legacy_flags_timeseries_index ON flags.legacy USING HASH (timeseries);
