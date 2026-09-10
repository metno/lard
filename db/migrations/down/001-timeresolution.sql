ALTER TABLE timeseries
    DROP COLUMN IF EXISTS timeresolution,
    DROP COLUMN IF EXISTS timeresolution_assessed;