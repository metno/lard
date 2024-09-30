use crate::{
    permissions::{timeseries_is_open, ParamPermitTable, StationPermitTable},
    Datum, Error, Param, PooledPgConn,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use regex::Regex;
use std::{
    collections::HashMap,
    fmt::Debug,
    str::{FromStr, Lines},
    sync::{Arc, RwLock},
};

/// Represents a set of observations that came in the same message from obsinn, with shared
/// station_id and type_id
#[derive(Debug, PartialEq)]
pub struct ObsinnChunk<'a> {
    observations: Vec<ObsinnObs<'a>>,
    station_id: i32, // TODO: change name here to nationalnummer?
    type_id: i32,
}

/// Represents a single observation from an obsinn message
#[derive(Debug, PartialEq)]
pub struct ObsinnObs<'a> {
    // TODO: this timestamp is shared by all obs in the row, maybe we should have
    // a Vec<ObsType> here and remove the Vec<ObsinnObs> from ObsinnChunk
    timestamp: DateTime<Utc>,
    id: ObsinnId,
    value: ObsType<'a>,
}

/// Represents the different Data types observation can have
#[derive(Debug, PartialEq)]
pub enum ObsType<'a> {
    Scalar(f32),
    NonScalar(&'a str),
}

/// Identifier for a single observation within a given obsinn message
#[derive(Debug, Clone, PartialEq)]
struct ObsinnId {
    param_code: String,
    sensor_and_level: Option<(i32, i32)>,
}

// TODO: maybe this can be a field in ObsinnChunk?
struct ObsinnHeader<'a> {
    station_id: i32,
    type_id: i32,
    message_id: usize,
    // Optional field with the timestamp when the data in the message was received by Obsinn
    // TODO: we can parse it to Datatime if we decide we are going to use it
    received_time: Option<&'a str>,
}

impl<'a> ObsinnHeader<'a> {
    fn parse(meta: &'a str) -> Result<Self, Error> {
        let mut fields = meta.split('/');

        let kldata_string = fields
            .next()
            .ok_or_else(|| Error::Parse("kldata header terminated early".to_string()))?;

        if kldata_string != "kldata" {
            return Err(Error::Parse(
                "kldata indicator missing or out of order".to_string(),
            ));
        }

        let unexpected_field_error = |field: &str| {
            Error::Parse(format!(
                "unexpected field in kldata header format: {}",
                field
            ))
        };

        let mut station_id: Option<i32> = None;
        let mut type_id: Option<i32> = None;
        let mut message_id: Option<usize> = None;
        let mut received_time: Option<&str> = None;

        for field in fields.by_ref() {
            // TODO: this field signals data deletion/update in kvalobs, we do not use it
            if field == "add" {
                continue;
            }

            let (key, value) = field
                .split_once('=')
                .ok_or_else(|| unexpected_field_error(field))?;

            // TODO: check possible ordering by logging incoming messages
            match key {
                "nationalnr" => station_id = Some(parse_value(key, value)?),
                "type" => type_id = Some(parse_value(key, value)?),
                "messageid" => message_id = Some(parse_value(key, value)?),
                "received_time" => received_time = Some(value),
                _ => return Err(unexpected_field_error(field)),
            }
        }

        Ok(ObsinnHeader {
            station_id: station_id.ok_or(Error::Parse(
                "missing field `nationalnr` in kldata header".to_string(),
            ))?,
            type_id: type_id.ok_or(Error::Parse(
                "missing field `type` in kldata header".to_string(),
            ))?,
            message_id: message_id.unwrap_or(0),
            received_time,
        })
    }
}

fn parse_value<T: FromStr>(key: &str, value: &str) -> Result<T, Error>
where
    <T as FromStr>::Err: Debug,
{
    value.parse::<T>().map_err(|_| {
        Error::Parse(format!(
            "invalid value {} in kldata header for key {}",
            value, key
        ))
    })
}

fn parse_columns(cols_raw: &str) -> Result<Vec<ObsinnId>, Error> {
    // this regex is taken from kvkafka's kldata parser
    // let col_regex = Regex::new(r"([^(),]+)(\([0-9]+,[0-9]+\))?").unwrap();
    // It matches all comma separated fields with pattern of type `name` and `name(x,y)`,
    // where `x` and `y` are ints
    // it is modified below to capture sensor and level separately, while keeping
    // the block collectively optional

    // TODO: is it possible to reuse this regex even more?
    let col_regex = Regex::new(r"([^(),]+)(\(([0-9]+),([0-9]+)\))?").unwrap();

    // TODO: gracefully handle errors here? Even though this shouldn't really ever panic?
    col_regex
        .captures_iter(cols_raw)
        .map(|caps| {
            Ok(ObsinnId {
                param_code: caps.get(1).unwrap().as_str().to_owned(),
                sensor_and_level: caps.get(2).map(|_| {
                    (
                        caps.get(3).unwrap().as_str().parse().unwrap(),
                        caps.get(4).unwrap().as_str().parse().unwrap(),
                    )
                }),
            })
        })
        .collect::<Result<Vec<ObsinnId>, Error>>()
}

fn parse_obs<'a>(
    csv_body: Lines<'a>,
    columns: &[ObsinnId],
    reference_params: Arc<HashMap<String, Param>>,
) -> Result<Vec<ObsinnObs<'a>>, Error> {
    let mut obs = Vec::new();
    let row_is_empty = || Error::Parse("empty row in kldata csv".to_string());

    for row in csv_body {
        let (timestamp, vals) = {
            let mut vals = row.split(',');

            let raw_timestamp = vals.next().ok_or_else(row_is_empty)?;

            // TODO: timestamp parsing needs to handle milliseconds and truncated timestamps?
            let timestamp = NaiveDateTime::parse_from_str(raw_timestamp, "%Y%m%d%H%M%S")
                .map_err(|e| Error::Parse(e.to_string()))?
                .and_utc();

            (timestamp, vals)
        };

        for (i, val) in vals.enumerate() {
            // TODO: should we do some smart bounds-checking??
            let col = columns[i].clone();

            let value = match reference_params.get(&col.param_code) {
                Some(ref_param) => {
                    if ref_param.is_scalar {
                        // NOTE: we assume ref_params marked as scalar in Stinfosys to be floats (but
                        // could be ints, which wouldn't be ideal)
                        let parsed = val.parse().map_err(|_| {
                            Error::Parse(format!("value {} could not be parsed as float", val))
                        })?;

                        ObsType::Scalar(parsed)
                    } else {
                        // TODO: we should implement logging/tracing sooner or later
                        println!(
                            "non-scalar param ({}, {}, {}): '{}'",
                            ref_param.id, col.param_code, ref_param.element_id, val
                        );

                        ObsType::NonScalar(val)
                    }
                }
                None => {
                    println!("unrecognised param_code {}: '{}'", col.param_code, val);
                    ObsType::NonScalar(val)
                }
            };

            obs.push(ObsinnObs {
                timestamp,
                id: col,
                value,
            })
        }
    }

    if obs.is_empty() {
        return Err(row_is_empty());
    }

    Ok(obs)
}

pub fn parse_kldata(
    msg: &str,
    param_conversions: Arc<HashMap<String, Param>>,
) -> Result<(usize, ObsinnChunk), Error> {
    let mut csv_body = msg.lines();
    let lines_err = || Error::Parse("kldata message contained too few lines".to_string());

    // parse the first two lines of the message as meta header, and csv column names,
    // leave the rest as an iter over the lines of csv body
    let header = ObsinnHeader::parse(csv_body.next().ok_or_else(lines_err)?)?;
    let columns = parse_columns(csv_body.next().ok_or_else(lines_err)?)?;

    Ok((
        header.message_id,
        ObsinnChunk {
            observations: parse_obs(csv_body, &columns, param_conversions)?,
            station_id: header.station_id,
            type_id: header.type_id,
        },
    ))
}

// TODO: rewrite such that queries can be pipelined?
// not pipelining here hurts latency, but shouldn't matter for throughput
pub async fn filter_and_label_kldata<'a>(
    chunk: ObsinnChunk<'a>,
    conn: &mut PooledPgConn<'_>,
    param_conversions: Arc<HashMap<String, Param>>,
    permit_table: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<Vec<Datum<'a>>, Error> {
    let query_get_obsinn = conn
        .prepare(
            "SELECT timeseries \
                FROM labels.obsinn \
                WHERE nationalnummer = $1 \
                    AND type_id = $2 \
                    AND param_code = $3 \
                    AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4)) \
                    AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))",
        )
        .await?;

    let mut data = Vec::with_capacity(chunk.observations.len());

    for in_datum in chunk.observations {
        // get the conversion first, so we avoid wasting a tsid if it doesn't exist
        let param = param_conversions
            .get(&in_datum.id.param_code)
            .ok_or_else(|| {
                Error::Parse(format!(
                    "unrecognised param_code '{}'",
                    in_datum.id.param_code
                ))
            })?;

        // TODO: we only need to check inside this loop if station_id is in the
        // param_permit_table
        if !timeseries_is_open(
            permit_table.clone(),
            chunk.station_id,
            chunk.type_id,
            param.id,
        )? {
            // TODO: log that the timeseries is closed? Mostly useful for tests
            #[cfg(feature = "integration_tests")]
            eprintln!("station {}: timeseries is closed", chunk.station_id);
            continue;
        }

        let transaction = conn.transaction().await?;

        let (sensor, lvl) = in_datum
            .id
            .sensor_and_level
            .map(|both| (Some(both.0), Some(both.1)))
            .unwrap_or((None, None));

        let obsinn_label_result = transaction
            .query_opt(
                &query_get_obsinn,
                &[
                    &chunk.station_id,
                    &chunk.type_id,
                    &in_datum.id.param_code,
                    &lvl,
                    &sensor,
                ],
            )
            .await?;

        let timeseries_id: i32 = match obsinn_label_result {
            Some(row) => row.get(0),
            None => {
                // create new timeseries
                // TODO: currently we create a timeseries with null location
                // In the future the location column should be moved to the timeseries metadata table
                let timeseries_id = transaction
                    .query_one(
                        "INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id",
                        &[&in_datum.timestamp],
                    )
                    .await?
                    .get(0);

                // create obsinn label
                transaction
                    .execute(
                        "INSERT INTO labels.obsinn \
                                (timeseries, nationalnummer, type_id, param_code, lvl, sensor) \
                            VALUES ($1, $2, $3, $4, $5, $6)",
                        &[
                            &timeseries_id,
                            &chunk.station_id,
                            &chunk.type_id,
                            &in_datum.id.param_code,
                            &lvl,
                            &sensor,
                        ],
                    )
                    .await?;

                // create met label
                transaction
                    .execute(
                        "INSERT INTO labels.met \
                                (timeseries, station_id, param_id, type_id, lvl, sensor) \
                            VALUES ($1, $2, $3, $4, $5, $6)",
                        &[
                            &timeseries_id,
                            &chunk.station_id,
                            &param.id,
                            &chunk.type_id,
                            &lvl,
                            &sensor,
                        ],
                    )
                    .await?;

                timeseries_id
            }
        };

        transaction.commit().await?;

        data.push(Datum {
            timeseries_id,
            timestamp: in_datum.timestamp,
            value: in_datum.value,
        });
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use crate::get_conversions;
    use chrono::TimeZone;
    use test_case::test_case;

    use super::ObsType::{NonScalar, Scalar};
    use super::*;

    #[test_case(
        "Test message that fails." => Err(Error::Parse("kldata indicator missing or out of order".to_string()));
        "missing kldata indicator"
    )]
    // TODO: missing messageid defaults to 0
    #[test_case(
        "kldata/nationalnr=100/type=504" => Ok((100, 504, 0));
        "valid header 1"
    )]
    #[test_case(
        "kldata/type=504/nationalnr=100/messageid=25" => Ok((100, 504, 25));
        "valid header 2"
    )]
    #[test_case(
        "kldata/messageid=23/nationalnr=99993/type=508/add" => Ok((99993, 508, 23));
        "valid header 3"
    )]
    #[test_case(
        "kldata/received_time=\"2024-07-05 08:27:40+00\"/nationalnr=297000/type=70051" => Ok((297000, 70051, 0));
        "valid header 4"
    )]
    #[test_case(
        "kldata/nationalnr=93140/type=501/unexpected" => Err(Error::Parse("unexpected field in kldata header format: unexpected".to_string()));
        "unexpected field"
    )]
    #[test_case(
        "kldata/messageid=10/type=501" => Err(Error::Parse("missing field `nationalnr` in kldata header".to_string()));
        "missing nationlnr"
    )]
    #[test_case(
        "kldata/messageid=10/nationalnr=93140" => Err(Error::Parse("missing field `type` in kldata header".to_string()));
        "missing type"
    )]
    fn test_parse_meta(msg: &str) -> Result<(i32, i32, usize), Error> {
        let header = ObsinnHeader::parse(msg)?;

        Ok((header.station_id, header.type_id, header.message_id))
    }

    #[test_case(
        "KLOBS,QSI_01(0,0)" => Ok(vec![
            ObsinnId{param_code: "KLOBS".to_string(), sensor_and_level: None},
            ObsinnId{param_code: "QSI_01".to_string(), sensor_and_level: Some((0,0))}
        ]);
        "match 1"
    )]
    #[test_case(
        "param_1,param_2,QSI_01(0,0)" => Ok(vec![
            ObsinnId{param_code: "param_1".to_string(), sensor_and_level: None},
            ObsinnId{param_code: "param_2".to_string(), sensor_and_level: None},
            ObsinnId{param_code: "QSI_01".to_string(), sensor_and_level: Some((0,0))}
        ]);
        "match 2"
    )]
    #[test_case(
        "param_1(0,0),param_2,param_3(0,0)" => Ok(vec![
            ObsinnId{param_code: "param_1".to_string(), sensor_and_level: Some((0,0))},
            ObsinnId{param_code: "param_2".to_string(), sensor_and_level: None},
            ObsinnId{param_code: "param_3".to_string(), sensor_and_level: Some((0,0))}
        ]);
        "match 3"
    )]
    // NOTE: cases not taken into account here
    // - "()"             => Vec::new()
    // - "param(0.1,0)" => vec[param, 0.1, 0]
    // - "param(0,0.1)" => vec[param, 0.1, 0]
    fn test_parse_columns(cols: &str) -> Result<Vec<ObsinnId>, Error> {
        parse_columns(cols)
    }

    #[test_case(
        "20160201054100,-1.1,0,2.80",
        &[
            ObsinnId{param_code: "TA".to_string(), sensor_and_level: None},
            ObsinnId{param_code: "CI".to_string(), sensor_and_level: None},
            ObsinnId{param_code: "IR".to_string(), sensor_and_level: None},
        ] => Ok(vec![
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2016,2, 1, 5, 41, 0).unwrap(),
                id: ObsinnId{param_code: "TA".to_string(), sensor_and_level: None}, 
                value: Scalar(-1.1)
            },
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2016,2, 1, 5, 41, 0).unwrap(),
                id: ObsinnId{param_code: "CI".to_string(), sensor_and_level: None}, 
                value: Scalar(0.0)
            },
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2016,2, 1, 5, 41, 0).unwrap(),
                id: ObsinnId{param_code: "IR".to_string(), sensor_and_level: None}, 
                value: Scalar(2.8)
            },
        ]);
        "single line"
    )]
    #[test_case(
        "20160201054100,-1.1,0,2.80\n20160201055100,-1.5,1,2.90",
        &[
            ObsinnId{param_code: "TA".to_string(), sensor_and_level: None},
            ObsinnId{param_code: "CI".to_string(), sensor_and_level: None},
            ObsinnId{param_code: "IR".to_string(), sensor_and_level: None},
        ] => Ok(vec![
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2016,2, 1, 5, 41, 0).unwrap(),
                id: ObsinnId{param_code: "TA".to_string(), sensor_and_level: None}, 
                value: Scalar(-1.1)
            },
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2016,2, 1, 5, 41, 0).unwrap(),
                id: ObsinnId{param_code: "CI".to_string(), sensor_and_level: None}, 
                value: Scalar(0.0)
            },
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2016,2, 1, 5, 41, 0).unwrap(),
                id: ObsinnId{param_code: "IR".to_string(), sensor_and_level: None}, 
                value: Scalar(2.8)
            },
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2016,2, 1, 5, 51, 0).unwrap(),
                id: ObsinnId{param_code: "TA".to_string(), sensor_and_level: None}, 
                value: Scalar(-1.5)
            },
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2016,2, 1, 5, 51, 0).unwrap(),
                id: ObsinnId{param_code: "CI".to_string(), sensor_and_level: None}, 
                value: Scalar(1.0)
            },
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2016,2, 1, 5, 51, 0).unwrap(),
                id: ObsinnId{param_code: "IR".to_string(), sensor_and_level: None}, 
                value: Scalar(2.9)
            },
        ]);
        "multiple lines"
    )]
    #[test_case("20240910000000,20240910000000,10.1",
        &[
            ObsinnId{param_code: "KLOBS".to_string(), sensor_and_level: None},
            ObsinnId{param_code: "TA".to_string(), sensor_and_level: None},
        ] => Ok(vec![
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2024, 9, 10, 0, 0, 0).unwrap(),
                id: ObsinnId{param_code: "KLOBS".to_string(), sensor_and_level: None}, 
                value: NonScalar("20240910000000") 
            },
            ObsinnObs{
                timestamp: Utc.with_ymd_and_hms(2024, 9, 10, 0, 0, 0).unwrap(),
                id: ObsinnId{param_code: "TA".to_string(), sensor_and_level: None}, 
                value: Scalar(10.1)
            }]
        );
        "non scalar parameter"
    )]
    #[test_case("20240910000000,20240910000000,10.1",
        &[
            ObsinnId{param_code: "unknown".to_string(), sensor_and_level: None},
            ObsinnId{param_code: "TA".to_string(), sensor_and_level: None},
        ] => Err(Error::Parse("unrecognised param_code 'unknown'".to_string()));
        "unrecognised param code"
    )]
    fn test_parse_obs<'a>(data: &'a str, cols: &[ObsinnId]) -> Result<Vec<ObsinnObs<'a>>, Error> {
        let param_conversions = get_conversions("resources/paramconversions.csv").unwrap();
        parse_obs(data.lines(), cols, param_conversions)
    }

    // NOTE: just test for basic failures, the happy path should already be captured by the other tests
    #[test_case(
        "" => Err(Error::Parse("kldata message contained too few lines".to_string()));
        "empty line"
    )]
    #[test_case(
        "kldata/nationalnr=99993/type=508/messageid=23" => Err(Error::Parse("kldata message contained too few lines".to_string()));
        "header only"
    )]
    #[test_case(
        "kldata/nationalnr=93140/type=501/messageid=23
DD(0,0),FF(0,0),DG_1(0,0),FG_1(0,0),KLFG_1(0,0),FX_1(0,0)" => Err(Error::Parse("empty row in kldata csv".to_string()));
        "missing data"
    )]
    fn test_parse_kldata(body: &str) -> Result<(usize, ObsinnChunk), Error> {
        let param_conversions = get_conversions("resources/paramconversions.csv").unwrap();
        parse_kldata(body, param_conversions)
    }
}
