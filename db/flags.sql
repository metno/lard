CREATE SCHEMA IF NOT EXISTS flags;

CREATE TABLE IF NOT EXISTS flags.kvdata (
    timeseries INT4 PRIMARY KEY REFERENCES public.timeseries,
    obstime TIMESTAMPTZ NOT NULL,
    original REAL NULL, -- could decide not to store this in the future? (KDVH migration will not contain this)
    corrected REAL NULL,
    controlinfo TEXT NULL,
    useinfo TEXT NULL,
    cfailed INT4 NULL
);
CREATE INDEX IF NOT EXISTS kvdata_obstime_index ON flags.kvdata (obstime); 
-- the timeseries will get an index since it is the primary key
