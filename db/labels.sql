CREATE SCHEMA IF NOT EXISTS labels;

CREATE TABLE labels.filter (
    timeseries INT4 PRIMARY KEY REFERENCES public.timeseries,
    stationID REAL,
    elementID TEXT,
    lvl INT4,
    sensor INT4
);
CREATE INDEX filter_station_element_index ON labels.filter (stationID, elementID);
