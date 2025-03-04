CREATE SCHEMA IF NOT EXISTS labels;

-- TODO: Should there be another unique contraint on this?
CREATE TABLE IF NOT EXISTS labels.met (
    timeseries INT8 PRIMARY KEY REFERENCES public.timeseries,
    station_id INT4,
    param_id INT4,
    -- TODO: Maybe change this as we reevaluate type_id's usefulness and future at met?
    type_id INT4,
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

-- This table holds extra metadata for a timeseries that was imported from kvalobs
-- TODO: db, tbl and import_* can be dangerous (?), kvalobs only keeps the last three months of data
CREATE TABLE IF NOT EXISTS labels.kvalobs (
    timeseries INT8 PRIMARY KEY REFERENCES public.timeseries,
    station_id INT4,
    param_id INT4,
    type_id INT4,
    lvl INT4,
    sensor INT4,
    -- Database where the timeseries was imported from
    -- Either 'kvalobs' or 'histkvalobs'
    db TEXT,
    -- Table in the database where the timeseries comes from
    -- Either `data` or `text_data`
    tbl TEXT,
    -- Time range of the dumped data
    import_from DATE,
    import_to DATE
);
CREATE INDEX IF NOT EXISTS kvalobs_label_index ON labels.kvalobs (db, tbl, import_from);
