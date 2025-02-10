use crate::{
    permissions::{timeseries_is_open, ParamPermitTable, StationPermitTable},
    DataChunk, Datum, Error, ObsType, PooledPgConn, ReferenceParam,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use chronoutil::RelativeDuration;
use regex::Regex;
use std::{
    collections::HashMap,
    fmt::Debug,
    str::{FromStr, Lines},
    sync::{Arc, RwLock},
};

// TODO: remove
/// List of non scalar param codes we don't need to log since we already know their type
const EXCLUDE_TEXT_LOG: [&str; 3] = [
    "KLOBS",     // timestamp
    "signature", // text
    "WWB1",      // text, metar
];

/// FIXME: these params are scalar in Stinfosys, but are not when coming from Obsinn.
/// - The first five are METAR params that come in as 'xxL' and 'xxR', where 'x' is a numeric character.
///   We need to decide how to treat them (Kvalobs silently discards them apparently)
///   Or if they need to be changed in Stinfosys
/// - The last one (W1) seems to be a number most of the times, but gets an 'a' every once in a while.
///   Maybe it's in hex format?
const SPECIAL_CASES: [&str; 6] = ["X1R", "X2R", "X3R", "WS", "WS2", "W1"];

/// Represents a set of observations that came in the same message from obsinn, with shared
/// station_id and type_id
#[derive(Debug, PartialEq)]
pub struct ObsinnChunk<'a> {
    observations: Vec<ObsinnObs<'a>>,
    station_id: i32, // TODO: change name here to nationalnummer?
    type_id: i32,
    timestamp: DateTime<Utc>,
}

/// Represents a single observation from an obsinn message
#[derive(Debug, PartialEq)]
pub struct ObsinnObs<'a> {
    id: ObsinnId,
    value: ObsType<'a>,
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
    _received_time: Option<&'a str>,
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
            _received_time: received_time,
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
                param_code: caps.get(1).unwrap().as_str().trim().to_owned(),
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

fn parse_scalar<'a>(val: &'a str, col: &ObsinnId) -> Result<ObsType<'a>, Error> {
    // NOTE(1): some params can be empty (old formats that were carried over
    // or a hacky way to have the observations deleted)
    // NOTE(2): some params can be simply "-" instead of being empty (hack?
    // Does it have a meaning?)
    if val.is_empty() || val == "-" {
        return Ok(ObsType::Scalar(None));
    }

    let parsed = match val.parse() {
        Ok(v) => v,
        Err(_) => {
            // TODO: should this fallback to inserting into the text table?
            let msg = format!(
                "value {} = {} could not be parsed as float",
                col.param_code, val
            );
            eprintln!("{msg}");
            return Err(Error::Parse(msg));
        }
    };

    Ok(ObsType::Scalar(Some(parsed)))
}

fn parse_nonscalar(val: &str) -> ObsType {
    if val.is_empty() || val == "-" {
        return ObsType::NonScalar(None);
    }

    ObsType::NonScalar(Some(val))
}

fn parse_obs<'a>(
    csv_body: Lines<'a>,
    columns: &[ObsinnId],
    reference_params: Arc<HashMap<String, ReferenceParam>>,
    header: ObsinnHeader<'a>,
) -> Result<Vec<ObsinnChunk<'a>>, Error> {
    let mut chunks = Vec::new();
    let row_is_empty = || Error::Parse("empty row in kldata csv".to_string());

    for row in csv_body {
        let mut obs = Vec::new();
        let (timestamp, vals) = {
            let mut vals = row.split(',').map(str::trim);

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
                // NOTE: we assume ref_params marked as scalar in Stinfosys to be floats (but
                // could be ints, which wouldn't be ideal?)
                Some(ref_param) => {
                    if ref_param.is_scalar && !SPECIAL_CASES.contains(&col.param_code.as_str()) {
                        parse_scalar(val, &col)?
                    } else {
                        if !EXCLUDE_TEXT_LOG.contains(&col.param_code.as_str()) {
                            eprintln!(
                                "non-scalar param ({}, {}, {}): '{}'",
                                ref_param.id, col.param_code, ref_param.element_id, val
                            );
                        }

                        parse_nonscalar(val)
                    }
                }
                None => {
                    eprintln!("unrecognised param_code '{}': '{}'", col.param_code, val);
                    // TODO: the only problem with this is that number-like
                    // values (eg timestamps) can be parsed correctly
                    // We would need a parse chain to handle different types
                    // match val.parse() {
                    //     Ok(parsed) => ObsType::Scalar(Some(parsed)),
                    //     Err(_) => ObsType::NonScalar(val),
                    // }
                    parse_nonscalar(val)
                }
            };

            obs.push(ObsinnObs { id: col, value })
        }

        // TODO: should this be more resiliant?
        if obs.is_empty() {
            return Err(row_is_empty());
        }

        chunks.push(ObsinnChunk {
            observations: obs,
            station_id: header.station_id,
            type_id: header.type_id,
            timestamp,
        })
    }

    Ok(chunks)
}

pub fn parse_kldata(
    msg: &str,
    reference_params: Arc<HashMap<String, ReferenceParam>>,
) -> Result<(usize, Vec<ObsinnChunk>), Error> {
    let (header, columns, csv_body) = {
        let mut csv_body = msg.lines();
        let lines_err = || Error::Parse("kldata message contained too few lines".to_string());

        // parse the first two lines of the message as meta header, and csv column names,
        // leave the rest as an iter over the lines of csv body
        let header = ObsinnHeader::parse(csv_body.next().ok_or_else(lines_err)?)?;
        let columns = parse_columns(csv_body.next().ok_or_else(lines_err)?)?;

        (header, columns, csv_body)
    };

    Ok((
        header.message_id,
        parse_obs(csv_body, &columns, reference_params, header)?,
    ))
}

// TODO: this is a messy hack, but it's the only way people at met currently have to determine
// time_resolution. Ultimately we intend to store time_resolution info in the database under
// public.timeseries or labels.met. This will be populated by a combination of a script that looks
// at a timeseries's history, and manual editing by content managers.
pub fn type_id_to_time_resolution(type_id: i32) -> Option<RelativeDuration> {
    // Source for these matches: PDF presented by PiM
    match type_id {
        514 => Some(RelativeDuration::minutes(1)),
        506 | 509 | 510 => Some(RelativeDuration::minutes(10)),
        7 | 311 | 330 | 342 | 501 | 502 | 503 | 505 | 507 | 511 => Some(RelativeDuration::hours(1)),
        522 => Some(RelativeDuration::days(1)),
        399 => Some(RelativeDuration::years(1)),
        _ => None,
    }
}

// TODO: rewrite such that queries can be pipelined?
// not pipelining here hurts latency, but shouldn't matter for throughput
pub async fn filter_and_label_kldata<'a>(
    chunks: Vec<ObsinnChunk<'a>>,
    conn: &mut PooledPgConn<'_>,
    param_conversions: Arc<HashMap<String, ReferenceParam>>,
    permit_table: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<Vec<DataChunk<'a>>, Error> {
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

    let mut out_chunks = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let mut data = Vec::with_capacity(chunk.observations.len());

        for in_datum in chunk.observations {
            let param_id = param_conversions
                .get(&in_datum.id.param_code)
                .map(|param| param.id);

            // TODO: With some changes to this function, we could potentially move its call outside
            // the loop body. For one thing, the station permit checks, if done in a separate
            // function, would apply to all observations. Since we know the param-specific permits
            // are barely used, we could also pre-emptively check all param permits outside the
            // loop.
            if !timeseries_is_open(
                permit_table.clone(),
                chunk.station_id,
                chunk.type_id,
                param_id,
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

            let timeseries_id: i64 = match obsinn_label_result {
                Some(row) => row.get(0),
                None => {
                    // create new timeseries
                    // TODO: currently we create a timeseries with null location
                    // In the future the location column should be moved to the timeseries metadata table
                    let timeseries_id = transaction
                        .query_one(
                            "INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id",
                            &[&chunk.timestamp],
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
                                &param_id,
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
                param_id,
                value: in_datum.value,
                // default to true as this means no QC failure, this will be mutated later if a
                // pipeline fails
                qc_usable: true,
            });
        }
        out_chunks.push(DataChunk {
            timestamp: chunk.timestamp,
            // TODO: real time_resolution (derive from type_id for now)
            time_resolution: type_id_to_time_resolution(chunk.type_id),
            data,
        });
    }

    Ok(out_chunks)
}

#[cfg(test)]
mod tests {
    use crate::get_conversions;
    use chrono::TimeZone;

    use super::ObsType::{NonScalar, Scalar};
    use super::*;

    #[test]
    fn test_parse_meta() {
        let cases = vec![
            (
                "Test message that fails.",
                Err(Error::Parse(
                    "kldata indicator missing or out of order".to_string(),
                )),
                "missing kldata indicator",
            ),
            // TODO: missing messageid defaults to 0
            (
                "kldata/nationalnr=100/type=504",
                Ok((100, 504, 0)),
                "valid header 1",
            ),
            (
                "kldata/type=504/nationalnr=100/messageid=25",
                Ok((100, 504, 25)),
                "valid header 2",
            ),
            (
                "kldata/messageid=23/nationalnr=99993/type=508/add",
                Ok((99993, 508, 23)),
                "valid header 3",
            ),
            (
                "kldata/received_time=\"2024-07-05 08:27:40+00\"/nationalnr=297000/type=70051",
                Ok((297000, 70051, 0)),
                "valid header 4",
            ),
            (
                "kldata/nationalnr=93140/type=501/unexpected",
                Err(Error::Parse(
                    "unexpected field in kldata header format: unexpected".to_string(),
                )),
                "unexpected field",
            ),
            (
                "kldata/messageid=10/type=501",
                Err(Error::Parse(
                    "missing field `nationalnr` in kldata header".to_string(),
                )),
                "missing nationlnr",
            ),
            (
                "kldata/messageid=10/nationalnr=93140",
                Err(Error::Parse(
                    "missing field `type` in kldata header".to_string(),
                )),
                "missing type",
            ),
        ];
        for (msg, expected, case_description) in cases {
            let output = ObsinnHeader::parse(msg)
                .map(|header| (header.station_id, header.type_id, header.message_id));
            assert_eq!(output, expected, "{}", case_description);
        }
    }

    // NOTE: cases not taken into account here
    // - "()"             => Vec::new()
    // - "param(0.1,0)" => vec[param, 0.1, 0]
    // - "param(0,0.1)" => vec[param, 0.1, 0]
    #[test]
    fn test_parse_columns() {
        let cases = vec![
            (
                "KLOBS,QSI_01(0,0)",
                Ok(vec![
                    ObsinnId {
                        param_code: "KLOBS".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "QSI_01".to_string(),
                        sensor_and_level: Some((0, 0)),
                    },
                ]),
                "match 1",
            ),
            (
                "param_1,param_2,QSI_01(0,0)",
                Ok(vec![
                    ObsinnId {
                        param_code: "param_1".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "param_2".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "QSI_01".to_string(),
                        sensor_and_level: Some((0, 0)),
                    },
                ]),
                "match 2",
            ),
            (
                "param_1(0,0),param_2,param_3(0,0)",
                Ok(vec![
                    ObsinnId {
                        param_code: "param_1".to_string(),
                        sensor_and_level: Some((0, 0)),
                    },
                    ObsinnId {
                        param_code: "param_2".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "param_3".to_string(),
                        sensor_and_level: Some((0, 0)),
                    },
                ]),
                "match 3",
            ),
            (
                " TA(1,1), TAX,TAN ,TD (0,0)",
                Ok(vec![
                    ObsinnId {
                        param_code: "TA".to_string(),
                        sensor_and_level: Some((1, 1)),
                    },
                    ObsinnId {
                        param_code: "TAX".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "TAN".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "TD".to_string(),
                        sensor_and_level: Some((0, 0)),
                    },
                ]),
                "trim test",
            ),
        ];

        for (cols, expected, case_description) in cases {
            let output = parse_columns(cols);
            assert_eq!(output, expected, "{}", case_description);
        }
    }

    #[test]
    fn test_parse_obs() {
        let cases = vec![
            (
                "20160201054100,-1.1,0,2.80",
                vec![
                    ObsinnId {
                        param_code: "TA".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "CI".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "IR".to_string(),
                        sensor_and_level: None,
                    },
                ],
                ObsinnHeader {
                    station_id: 18700,
                    type_id: 511,
                    message_id: 1,
                    _received_time: None,
                },
                Ok(vec![ObsinnChunk {
                    observations: vec![
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "TA".to_string(),
                                sensor_and_level: None,
                            },
                            value: Scalar(Some(-1.1)),
                        },
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "CI".to_string(),
                                sensor_and_level: None,
                            },
                            value: Scalar(Some(0.0)),
                        },
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "IR".to_string(),
                                sensor_and_level: None,
                            },
                            value: Scalar(Some(2.8)),
                        },
                    ],
                    timestamp: Utc.with_ymd_and_hms(2016, 2, 1, 5, 41, 0).unwrap(),
                    station_id: 18700,
                    type_id: 511,
                }]),
                "single line",
            ),
            (
                "20160201054100,-1.1,0,2.80\n20160201055100,-1.5,1,2.90",
                vec![
                    ObsinnId {
                        param_code: "TA".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "CI".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "IR".to_string(),
                        sensor_and_level: None,
                    },
                ],
                ObsinnHeader {
                    station_id: 18700,
                    type_id: 511,
                    message_id: 1,
                    _received_time: None,
                },
                Ok(vec![
                    ObsinnChunk {
                        observations: vec![
                            ObsinnObs {
                                id: ObsinnId {
                                    param_code: "TA".to_string(),
                                    sensor_and_level: None,
                                },
                                value: Scalar(Some(-1.1)),
                            },
                            ObsinnObs {
                                id: ObsinnId {
                                    param_code: "CI".to_string(),
                                    sensor_and_level: None,
                                },
                                value: Scalar(Some(0.0)),
                            },
                            ObsinnObs {
                                id: ObsinnId {
                                    param_code: "IR".to_string(),
                                    sensor_and_level: None,
                                },
                                value: Scalar(Some(2.8)),
                            },
                        ],
                        timestamp: Utc.with_ymd_and_hms(2016, 2, 1, 5, 41, 0).unwrap(),
                        station_id: 18700,
                        type_id: 511,
                    },
                    ObsinnChunk {
                        observations: vec![
                            ObsinnObs {
                                id: ObsinnId {
                                    param_code: "TA".to_string(),
                                    sensor_and_level: None,
                                },
                                value: Scalar(Some(-1.5)),
                            },
                            ObsinnObs {
                                id: ObsinnId {
                                    param_code: "CI".to_string(),
                                    sensor_and_level: None,
                                },
                                value: Scalar(Some(1.0)),
                            },
                            ObsinnObs {
                                id: ObsinnId {
                                    param_code: "IR".to_string(),
                                    sensor_and_level: None,
                                },
                                value: Scalar(Some(2.9)),
                            },
                        ],
                        timestamp: Utc.with_ymd_and_hms(2016, 2, 1, 5, 51, 0).unwrap(),
                        station_id: 18700,
                        type_id: 511,
                    },
                ]),
                "multiple lines",
            ),
            (
                "20240910000000,20240910000000,10.1",
                vec![
                    ObsinnId {
                        param_code: "KLOBS".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "TA".to_string(),
                        sensor_and_level: None,
                    },
                ],
                ObsinnHeader {
                    station_id: 18700,
                    type_id: 511,
                    message_id: 1,
                    _received_time: None,
                },
                Ok(vec![ObsinnChunk {
                    observations: vec![
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "KLOBS".to_string(),
                                sensor_and_level: None,
                            },
                            value: NonScalar(Some("20240910000000")),
                        },
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "TA".to_string(),
                                sensor_and_level: None,
                            },
                            value: Scalar(Some(10.1)),
                        },
                    ],
                    timestamp: Utc.with_ymd_and_hms(2024, 9, 10, 0, 0, 0).unwrap(),
                    station_id: 18700,
                    type_id: 511,
                }]),
                "non scalar parameter",
            ),
            (
                "20240910000000,20240910000000,10.1",
                vec![
                    ObsinnId {
                        param_code: "unknown".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "TA".to_string(),
                        sensor_and_level: None,
                    },
                ],
                ObsinnHeader {
                    station_id: 18700,
                    type_id: 511,
                    message_id: 1,
                    _received_time: None,
                },
                Ok(vec![ObsinnChunk {
                    observations: vec![
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "unknown".to_string(),
                                sensor_and_level: None,
                            },
                            value: NonScalar(Some("20240910000000")),
                        },
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "TA".to_string(),
                                sensor_and_level: None,
                            },
                            value: Scalar(Some(10.1)),
                        },
                    ],
                    timestamp: Utc.with_ymd_and_hms(2024, 9, 10, 0, 0, 0).unwrap(),
                    station_id: 18700,
                    type_id: 511,
                }]),
                "unrecognised param code",
            ),
            (
                "20240910000000,-0.50,,0.70,",
                vec![
                    ObsinnId {
                        param_code: "TA".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "RI_01".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "FG_01".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "FGN_01".to_string(),
                        sensor_and_level: None,
                    },
                ],
                ObsinnHeader {
                    station_id: 18700,
                    type_id: 511,
                    message_id: 1,
                    _received_time: None,
                },
                Ok(vec![ObsinnChunk {
                    observations: vec![
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "TA".to_string(),
                                sensor_and_level: None,
                            },
                            value: Scalar(Some(-0.50)),
                        },
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "RI_01".to_string(),
                                sensor_and_level: None,
                            },
                            value: Scalar(None),
                        },
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "FG_01".to_string(),
                                sensor_and_level: None,
                            },
                            value: Scalar(Some(0.70)),
                        },
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "FGN_01".to_string(),
                                sensor_and_level: None,
                            },
                            value: NonScalar(None),
                        },
                    ],
                    timestamp: Utc.with_ymd_and_hms(2024, 9, 10, 0, 0, 0).unwrap(),
                    station_id: 18700,
                    type_id: 511,
                }]),
                "parameter with missing observations",
            ),
            (
                "20240910000000,-,24R,24L",
                vec![
                    ObsinnId {
                        param_code: "TJ".to_string(),
                        sensor_and_level: Some((0, 3000)),
                    },
                    ObsinnId {
                        param_code: "X1R".to_string(),
                        sensor_and_level: None,
                    },
                    ObsinnId {
                        param_code: "X2R".to_string(),
                        sensor_and_level: None,
                    },
                ],
                ObsinnHeader {
                    station_id: 18700,
                    type_id: 511,
                    message_id: 1,
                    _received_time: None,
                },
                Ok(vec![ObsinnChunk {
                    observations: vec![
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "TJ".to_string(),
                                sensor_and_level: Some((0, 3000)),
                            },
                            value: Scalar(None),
                        },
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "X1R".to_string(),
                                sensor_and_level: None,
                            },
                            value: NonScalar(Some("24R")),
                        },
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "X2R".to_string(),
                                sensor_and_level: None,
                            },
                            value: NonScalar(Some("24L")),
                        },
                    ],
                    timestamp: Utc.with_ymd_and_hms(2024, 9, 10, 0, 0, 0).unwrap(),
                    station_id: 18700,
                    type_id: 511,
                }]),
                "special cases",
            ),
        ];

        let param_conversions = get_conversions("resources/paramconversions.csv").unwrap();
        for (data, cols, header, expected, case_description) in cases {
            let output = parse_obs(data.lines(), &cols, param_conversions.clone(), header);
            assert_eq!(output, expected, "{}", case_description);
        }
    }

    // NOTE: just test for basic failures, the happy path should already be captured by the other tests
    #[test]
    fn test_parse_kldata() {
        let cases = vec![
            (
                "",
                Err(Error::Parse(
                    "kldata message contained too few lines".to_string(),
                )),
                "empty line",
            ),
            (
                "kldata/nationalnr=99993/type=508/messageid=23",
                Err(Error::Parse(
                    "kldata message contained too few lines".to_string(),
                )),
                "header only",
            ),
        ];
        let param_conversions = get_conversions("resources/paramconversions.csv").unwrap();

        for (body, expected, case_description) in cases {
            let output = parse_kldata(body, param_conversions.clone());
            assert_eq!(output, expected, "{}", case_description);
        }
    }
}
