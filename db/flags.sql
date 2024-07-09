CREATE SCHEMA IF NOT EXISTS flags;

CREATE TABLE IF NOT EXISTS flags.kvdata (
    timeseries INT4 REFERENCES public.timeseries,
    obstime TIMESTAMPTZ NOT NULL,
    original REAL NULL, -- could decide not to store this in the future? (KDVH migration will not contain this)
    corrected REAL NULL,
    controlinfo TEXT NULL,
    useinfo TEXT NULL,
    cfailed INT4 NULL
);
-- TODO: Probably should define unique constraint on (timeseries, obstime) as we have in public.data?
-- Can kvkafka resend data with same (timeseries, obstime)?
CREATE INDEX IF NOT EXISTS kvdata_obtime_index ON flags.kvdata (obstime); 
CREATE INDEX IF NOT EXISTS kvdata_timeseries_index ON flags.kvdata USING HASH (timeseries); 
