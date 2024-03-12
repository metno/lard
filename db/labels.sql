CREATE SCHEMA IF NOT EXISTS labels;

CREATE TABLE labels.filter (
    timeseries INT4 PRIMARY KEY REFERENCES public.timeseries,
    station_id INT4,
    element_id TEXT,
    lvl INT4,
    sensor INT4
);
CREATE INDEX filter_station_element_index ON labels.filter (station_id, element_id);

CREATE TABLE labels.obsinn (
    timeseries INT4 PRIMARY KEY REFERENCES public.timeseries,
    nationalnummer INT4,
    type_id INT4,
    param_code TEXT,
    lvl INT4,
    sensor INT4
);
CREATE INDEX obsinn_all_index ON labels.filter (nationalnummer, type_id, param_code, lvl, sensor);
