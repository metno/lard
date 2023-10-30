CREATE SCHEMA IF NOT EXISTS labels;

CREATE TYPE filterlabel AS (
    stationID REAL,
    elementID TEXT,
    lvl INT4,
    sensor INT4
);

CREATE TABLE labels.filter (
    timeseries INT4 REFERENCES public.timeseries NOT NULL UNIQUE,
    label filterlabel NOT NULL
);