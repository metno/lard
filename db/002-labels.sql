CREATE SCHEMA IF NOT EXISTS labels;

-- TODO: Should there be another unique contraint on this?
CREATE TABLE IF NOT EXISTS labels.met (
    timeseries INT8 PRIMARY KEY REFERENCES public.timeseries,
    station_id INT4,
    param_id INT4,
    -- TODO: Maybe change this as we reevaluate type_id's usefulness and future at met?
    type_id INT4,
    -- Meteorological height in cm. NULL indicates level is missing or not
    -- relevant NOTE: this differs from semantics in ODA, kvalobs, stinfosys,
    -- et al. More info in ingestion/src/levels.rs
    lvl INT4,
    sensor INT4
);
CREATE INDEX IF NOT EXISTS met_station_element_index ON labels.met (station_id, param_id);

CREATE TABLE IF NOT EXISTS labels.obsinn (
    timeseries INT8 PRIMARY KEY REFERENCES public.timeseries,
    nationalnummer INT4,
    type_id INT4,
    param_code TEXT,
    lvl INT4,
    sensor INT4
);
CREATE INDEX IF NOT EXISTS obsinn_all_index ON labels.obsinn (nationalnummer, type_id, param_code, lvl, sensor);

CREATE TABLE IF NOT EXISTS labels.kdvh (
    timeseries INT8 PRIMARY KEY REFERENCES public.timeseries,
    station_id INT4 NOT NULL,
    type_id INT4,
    lvl INT4,
    sensor INT4,
    elem_code TEXT NOT NULL,
    -- Name of the KDVH table where this timeseries comes from
    tbl_name TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS kdvh_label_index ON labels.kdvh (tbl_name, station_id, elem_code);

CREATE TABLE IF NOT EXISTS labels.kvalobs (
    timeseries INT8 PRIMARY KEY REFERENCES public.timeseries,
    station_id INT4,
    param_id INT4,
    type_id INT4,
    lvl INT4,
    sensor INT4
);
CREATE INDEX IF NOT EXISTS kvalobs_label_index ON labels.kvalobs (station_id, param_id);
