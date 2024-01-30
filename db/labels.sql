CREATE SCHEMA IF NOT EXISTS labels;

CREATE TABLE IF NOT EXISTS labels.filter (
    timeseries INT4 PRIMARY KEY REFERENCES public.timeseries,
    station_id INT4,
    element_id TEXT,
    lvl INT4,
    sensor INT4
);
CREATE INDEX IF NOT EXISTS filter_station_element_index ON labels.filter (station_id, element_id);
