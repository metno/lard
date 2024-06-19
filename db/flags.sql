CREATE TABLE IF NOT EXISTS flags.kvdata (
    -- NOTE: this is the appropriate way to tie the QC data to a timeseries, or do we want to reference through a label somehow?
    timeseries INT4 PRIMARY KEY REFERENCES public.timeseries,
    obstime TIMESTAMPTZ NOT NULL,
    original REAL NULL,
    corrected REAL NULL,
    controlinfo TEXT NULL,
    useinfo TEXT NULL,
    cfailed INT4 NULL
);
-- TODO: could be slow to index on both of these? Maybe just want a timeseries index? 
CREATE INDEX IF NOT EXISTS kvdata_index ON flags.kvdata (timeseries, obstime); 
