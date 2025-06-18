CREATE SCHEMA IF NOT EXISTS flags;

-- TODO: should this also have a column for qc_time or some such?
CREATE TABLE IF NOT EXISTS flags.confident_provenance (
    timeseries INT8,
    obstime TIMESTAMPTZ,
    pipeline TEXT,
    -- TODO: should this be an enum?
    flag INT4 NOT NULL,
    -- TODO: better name? since this might be applied to flags that aren't fail but also aren't pass?
    fail_condition TEXT NULL,
    CONSTRAINT fk_confident_provenance_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries,
    PRIMARY KEY (timeseries, obstime, pipeline)
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS confident_provenance_timestamp_index ON flags.confident_provenance (obstime);

