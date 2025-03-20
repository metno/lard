# Database

The database is defined by a set of schemas defined in [/db](/db) each prefixed by a number defining the order in which they should be applied.

Of core importance is the `public` schema, where we introduce the concept of a `timeseries`:

```sql
CREATE TABLE IF NOT EXISTS public.timeseries (
    id SERIAL8 PRIMARY KEY,
    fromtime TIMESTAMPTZ NULL,
    totime TIMESTAMPTZ NULL,
    permit INT4 NULL,
    deactivated BOOL NULL
);

CREATE TABLE IF NOT EXISTS public.data (
    timeseries INT8 NOT NULL,
    obstime TIMESTAMPTZ NOT NULL,
    obsvalue FLOAT8,
    qc_usable BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT unique_data_timeseries_obstime UNIQUE (timeseries, obstime),
    CONSTRAINT fk_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries
) PARTITION BY RANGE (obstime);
```

A timeseries defines a sequential series of data points that go together, that typically implies that they come from the same instrument on the same station. The mapping from instrument to timeseries is not necessarily 1:1 though, as an instrument may get a new timeseries under certain circumstances, like the time resolution it reports at changing, or the station it belongs to changing location.

The actual data belonging to a timeseries is stored in `public.data` where a combination of a timeseries id and an obstime uniquely define an observation. In this table the values of observations are stored as floating point numbers, but we also have alternative tables built around other types for data that are not represented as floats. Which table a particular timeseries' observations should go in is conventionally determined by its parameter.

> **NOTE:** For the moment, we actually use `legacy.data` in place of `public.data`; this table is structurally similar, but includes QC information in the format used by [Kvalobs](https://github.com/metno/kvalobs). This is because we are currently reliant on QC information from Kvalobs until the output of the Confident project is production ready.

Moving stations (i.e. stations on ships) are supported by treating the stations' latitude and longitude as parameters, and giving them their own timeseries.

## Labels

TODO: labels

## Restricted data

TODO: restricted data
