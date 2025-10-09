package db

import "time"

// Kvalobs is composed of two databases
// 1) `kvalobs` for fresh data?
// 2) `histkvalobs` for data older than <not sure how long, 2-3 months?>
//
// Both contain the same tables:
// - `algorithms`: stores procedure code (!!!) for QC checks
// - `checks`: stores tags and signatures of QC tests
// - `data`: a view that joins `observations` and `obsvalue`
//
//       Column    |            Type             | Collation | Nullable |          Default
//    -------------+-----------------------------+-----------+----------+----------------------------
//     stationid   | integer                     |           | not null |
//     obstime     | timestamp without time zone |           | not null |
//     original    | double precision            |           | not null |
//     paramid     | integer                     |           | not null |
//     tbtime      | timestamp without time zone |           | not null |
//     typeid      | integer                     |           | not null |
//     sensor      | character(1)                |           |          | '0'::bpchar
//     level       | integer                     |           |          | 0
//     corrected   | double precision            |           | not null |
//     controlinfo | character(16)               |           |          | '0000000000000000'::bpchar
//     useinfo     | character(16)               |           |          | '0000000000000000'::bpchar
//     cfailed     | text                        |           |          |
//
// - `data_history`: stores the history of QC pipelines for data observations
//
// - `default_missing`:
// - `default_missing_values`: default values for some paramids (-32767)
// - `model`: stores model names
// - `model_data`: stores model data for different stations, paramids, etc.
//
// - `observations`: stores sequential observation IDs for each observations (note the lack of paramid)
//       Column     |            Type             | Collation | Nullable |
//   ---------------+-----------------------------+-----------+----------+
//    observationid | bigint                      |           | not null |
//    stationid     | integer                     |           | not null |
//    typeid        | integer                     |           | not null |
//    obstime       | timestamp without time zone |           | not null |
//    tbtime        | timestamp without time zone |           | not null |
//
// - `obsdata`: where the actual scalar data is stored
//       Column     |       Type       | Collation | Nullable |          Default
//   ---------------+------------------+-----------+----------+----------------------------
//    observationid | bigint           |           |          |
//    original      | double precision |           | not null |
//    paramid       | integer          |           | not null |
//    sensor        | character(1)     |           |          | '0'::bpchar
//    level         | integer          |           |          | 0
//    corrected     | double precision |           | not null |
//    controlinfo   | character(16)    |           |          | '0000000000000000'::bpchar
//    useinfo       | character(16)    |           |          | '0000000000000000'::bpchar
//    cfailed       | text             |           |          |
//
// - `obstextdata`: where the actual text data is stored
//       Column     |  Type   | Collation | Nullable | Default |
//   ---------------+---------+-----------+----------+---------+
//    observationid | bigint  |           |          |         |
//    original      | text    |           | not null |         |
//    paramid       | integer |           | not null |         |
//
// - `param`: part of stinfosys `param` table
//      Column    |  Type   | Collation | Nullable | Default
//   -------------+---------+-----------+----------+---------
//    paramid     | integer |           | not null |
//    name        | text    |           | not null |
//    description | text    |           |          |
//    unit        | text    |           |          |
//    level_scale | integer |           |          | 0
//    comment     | text    |           |          |
//    scalar      | boolean |           |          | true
//
// - `pdata`: view similar to `data` but with paramid converted to param code
// - `station`: station metadata such as (lat, lon, height, name, wmonr, etc)
// - `station_metadata`: Stores fromtime and totime for `stationid` and optionally `paramid`.
//                       `typeid`, `sensor`, and `level` are always NULL.
//
// - `text_data`: view that joins `observations` and `obstextdata`
//
//      Column   |            Type             | Collation | Nullable | Default
//    -----------+-----------------------------+-----------+----------+---------
//     stationid | integer                     |           | not null |
//     obstime   | timestamp without time zone |           | not null |
//     original  | text                        |           | not null |
//     paramid   | integer                     |           | not null |
//     tbtime    | timestamp without time zone |           | not null |
//     typeid    | integer                     |           | not null |
//
// - `text_data_history`: stores the history of QC pipelines for text observations ()
//
// IMPORTANT: considerations for migrations to LARD
//     - LARD stores Timeseries labels (stationid, paramid, typeid, sensor, level) in a separate table
//     - In LARD (sensor, level) can both be NULL, while in Kvalobs they have default values ('0',0)
//           => POSSIBLE INCONSISTENCY when importing to LARD
//     - Timestamps in Kvalobs are UTC
//     - Kvalobs doesn't have the concept of timeseries ID,
//       instead there is a sequential ID associated with each observation row

// Special values that are treated as NULL in Kvalobs
// TODO: are there more values we should be looking for?
var NULL_VALUES []float64 = []float64{-32767, -32766}

const DataTableName = "data"
const TextTableName = "text_data"

const KvDbName = "kvalobs"
const HistDbName = "histkvalobs"

var TABLES []string = []string{DataTableName, TextTableName}

const KvEnvVar = "KVALOBS_CONN_STRING"
const HistEnvVar = "HISTKVALOBS_CONN_STRING"

type DataSeries = []*DataObs

// Kvalobs data table observation row
type DataObs struct {
	Obstime     time.Time `db:"obstime"`
	Original    float64   `db:"original"`
	Tbtime      time.Time `db:"tbtime"`
	Corrected   float64   `db:"corrected"`
	Controlinfo *string   `db:"controlinfo"`
	Useinfo     *string   `db:"useinfo"`
	Cfailed     *string   `db:"cfailed"`
}

type TextSeries = []*TextObs
type StationType struct {
	Stationid int32
	Typeid    int32
}

// Kvalobs text_data table observation row
type TextObs struct {
	Obstime  time.Time `db:"obstime"`
	Original string    `db:"original"`
	Tbtime   time.Time `db:"tbtime"`
}
