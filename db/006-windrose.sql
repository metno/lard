DO $$
BEGIN
    IF (SELECT NOT EXISTS (select from pg_type where typname = 'windobs')) THEN
    -- Both speed and direction are `obsvalue`s from the data table.
    -- They are grouped in this type as convenience for calculation of the windrose report
    CREATE TYPE windobs AS (
        speed FLOAT8,
        direction FLOAT8
    );
END IF;
END $$;

DO $$
BEGIN
    IF (SELECT NOT EXISTS (select from pg_type where typname = 'windrose_result')) THEN
    -- Both speed and direction are `obsvalue`s from the data table.
    -- They are grouped in this type as convenience for calculation of the windrose report
    CREATE TYPE windrose_result AS (
        speed INT,
        direction INT,
        count INT,
        percent FLOAT8
    );
END IF;
END $$;

CREATE OR REPLACE FUNCTION windrose(speed_tsid INT8, direction_tsid INT8, fromtime TIMESTAMPTZ, totime TIMESTAMPTZ, months int[])
    RETURNS windrose_result
AS $$
    WITH wind_days AS (
        SELECT
            DATE_TRUNC('day', obstime) AS day,
            ARRAY_AGG((speed.obs, direction.obs)::windobs) AS obs
        FROM (
            SELECT obstime, corrected AS obs FROM legacy.data
            WHERE timeseries = speed_tsid
            AND corrected IS NOT NULL
            AND corrected > -30000.0
            AND quality_code IS NOT NULL
            AND quality_code != 7
            AND obstime >= fromtime AND obstime < totime
            AND EXTRACT(minute FROM obstime)::int = 0
            AND (months IS NULL OR EXTRACT(month FROM obstime)::int = ANY(months))
        ) speed
        INNER JOIN (
            SELECT obstime, corrected AS obs FROM legacy.data
            WHERE timeseries = direction_tsid
            AND corrected IS NOT NULL
            AND corrected > -30000.0
            AND quality_code IS NOT NULL
            AND quality_code != 7
            AND obstime >= fromtime AND obstime < totime
            AND EXTRACT(minute FROM obstime)::int = 0
            AND (months IS NULL OR EXTRACT(month FROM obstime)::int = ANY(months))
        ) direction
        USING (obstime)
        GROUP BY day
    ), unnested_values AS (
        SELECT day, unnest(obs) AS obs FROM wind_days
    ), day_stats AS (
        SELECT COUNT(*)::float8 AS n_days FROM wind_days
    ), obs_stats AS (
        SELECT day, ARRAY_LENGTH(obs, 1)::float8 AS n_obs FROM wind_days
    ), histogram AS (
        SELECT
            WIDTH_BUCKET((obs).speed, ARRAY[
                0.3, 1.6, 3.4, 5.5, 8.0, 10.8, 13.9, 17.2, 20.8, 24.5, 28.5, 32.6
            ]) AS speed,
            -- WIDTH_BUCKET((obs).direction, 11.25, 348.75, 15) % (15+1) as direction,
            WIDTH_BUCKET((obs).direction, ARRAY[
                0.0, 11.25, 33.75, 56.25, 78.75, 101.25, 123.75, 146.25, 168.75, 191.25, 213.75, 236.25, 258.75, 281.25, 303.75, 326.25, 348.75
            ]) AS direction,
            COUNT(*) as count,
            SUM(1.0 / n_obs) AS value
        FROM unnested_values
        JOIN obs_stats USING(day)
        GROUP BY speed, direction
        ORDER BY speed, direction
    )
    SELECT speed, direction, count, (value / n_days) AS percent FROM histogram, day_stats $$
LANGUAGE SQL;
