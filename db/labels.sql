CREATE SCHEMA IF NOT EXISTS labels;

CREATE TYPE filterlabel AS (
    stationID REAL,
    elementID TEXT,
    lvel integer,
    sensor integer
);

CREATE TABLE labels.filter (
    timeseries SERIAL REFERENCES public.timeseries NOT NULL UNIQUE,
    label filterlabel NOT NULL
);