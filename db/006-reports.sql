CREATE SCHEMA IF NOT EXISTS reports;

CREATE TABLE IF NOT EXISTS reports.idf_station_timeseries (
    id SERIAL4 PRIMARY KEY,
    station_id INT4 NOT NULL,
    number_of_seasons INT4 NOT NULL,
    quality_class INT4 NOT NULL,
    seed_parameter INT4 NOT NULL,
    -- code from stinfosys indicating rules for sharing data
    -- TODO: do we need this here?
    -- As far as I can see there are no restricted IDF timeseries?
    -- But that actually depends on the station_id
    permit INT4 NULL,
    fromtime TIMESTAMPTZ NOT NULL,
    totime TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS reports.idf_station_data (
    timeseries INT4 REFERENCES reports.idf_station_timeseries,
    duration INT4 NOT NULL,
    frequency INT4 NOT NULL,
    intensity FLOAT8 NOT NULL,
    lower_interval FLOAT8 NOT NULL,
    upper_interval FLOAT8 NOT NULL
);
