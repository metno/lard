CREATE SCHEMA IF NOT EXISTS labels;

-- TODO: Should there be another unique contraint on this?
CREATE TABLE labels.met (
    timeseries INT4 PRIMARY KEY REFERENCES public.timeseries,
    station_id INT4,
    param_id INT4,
    -- TODO: Maybe change this as we reevaluate type_id's usefulness and future at met?
    type_id INT4,
    lvl INT4,
    sensor INT4
);
CREATE INDEX met_station_element_index ON labels.met (station_id, param_id);

CREATE TABLE labels.obsinn (
    timeseries INT4 PRIMARY KEY REFERENCES public.timeseries,
    nationalnummer INT4,
    type_id INT4,
    param_code TEXT,
    lvl INT4,
    sensor INT4
);
CREATE INDEX obsinn_all_index ON labels.obsinn (nationalnummer, type_id, param_code, lvl, sensor);
