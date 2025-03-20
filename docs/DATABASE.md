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

The `labels` schema contains tables that add contextual information about timeseries that doesn't fit in `public.timeseries`. Generally we find 2 purposes for labels. The first is as a lookup aid to help users find the timeseries they're looking for. We use `labels.met` for this, which contains the keys end users at met typically need to look up timeseries:
```sql
CREATE TABLE IF NOT EXISTS labels.met (
    timeseries INT8 PRIMARY KEY REFERENCES public.timeseries,
    station_id INT4,
    param_id INT4,
    -- TODO: Maybe change this as we reevaluate type_id's usefulness and future at met?
    type_id INT4,
    lvl INT4,
    sensor INT4
);
```

The other purpose is source-specific labelling, where a label is used to tell you where the data in a timeseries came from, and include source-specific keys (such as alternative formats for identifying stations and parameters), that let you more easily collate between lard and the data source, if you need to check or correct the data's integrity. An example of such a label is `labels.obsinn`:
```sql
CREATE TABLE IF NOT EXISTS labels.obsinn (
    timeseries INT8 PRIMARY KEY REFERENCES public.timeseries,
    nationalnummer INT4,
    type_id INT4,
    param_code TEXT,
    lvl INT4,
    sensor INT4
);
```
which identifies parameters using a different format, `param_code`, that has an incomplete mapping to the `param_id` used in `labels.met`

## Restricted data

We support restricted data (data that has limitations or conditions on it's use such that it should not be available without access controls) by keeping it in a separate database (the [postgres concept called database](https://www.postgresql.org/docs/current/managing-databases.html) not a whole other postgres instance). This strikes a good balance of keeping the data separate enough (can't be accessed through the same connection), while not adding much maintenance burden (the database schemas are identical, and no new services are needed in the deployment). At ingestion time, we check with a metadata source (currently stinfosys) to determine the permit of the data (an integer where 1 indicates open data, other numbers indicated various restrictions) which is used to determine which database to ingest into.
