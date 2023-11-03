CREATE TYPE location AS (
    lat REAL,
    lon REAL,
    hamsl REAL,
    hag REAL
);

CREATE TABLE public.timeseries (
    id SERIAL PRIMARY KEY,
    fromtime TIMESTAMPTZ NULL,
    totime TIMESTAMPTZ NULL,
    loc location NULL, 
    deactivated BOOL NULL
);

CREATE TABLE public.data (
    timeseries INT4 NOT NULL,
    obstime TIMESTAMPTZ NOT NULL,
    obsvalue REAL,
    CONSTRAINT unique_data_timeseries_obstime UNIQUE (timeseries, obstime),
    CONSTRAINT fk_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries
) PARTITION BY RANGE (obstime);
CREATE INDEX timestamp_data_index ON public.data (obstime);
CREATE INDEX timeseries_data_index ON public.data USING HASH (timeseries);

/*
    TODO: 
    - Consider what to do with labels
      probably do a labels namespace with a table per labeltype
    - Figure out products
    - Figure out datagroups
    - Figure out RPC
    - Revisit purpose of timeseries.updatedat and timeseries.deactivated
*/
