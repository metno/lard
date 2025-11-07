use crate::{
    levels::{param_get_level, LevelTable},
    permissions::{timeseries_get_permit, PermitTables},
    DataChunk, Datum, Error, ObsType, ParamConversions, PooledPgConn, NONSCALAR_DATAPOINTS,
    SCALAR_DATAPOINTS,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use chronoutil::RelativeDuration;
use regex::Regex;
use std::{
    fmt::Debug,
    str::{FromStr, Lines},
};
use thiserror::Error as ThisError;
use tracing::{debug, info, warn};

#[derive(ThisError, Debug, PartialEq)]
pub enum ParseError {
    #[error("kldata message contained too few lines")]
    Lines,
    #[error("kldata header terminated early")]
    HeaderTermination,
    #[error("kldata indicator missing or out of order")]
    IndicatorMissing,
    #[error("unexpected field in kldata header format: {0}")]
    UnexpectedField(String),
    #[error("missing field `{0}` in kldata header")]
    MissingField(String),
    #[error("invalid value {0} in kldata header for key {1}")]
    InvalidValue(String, String),
    #[error("empty row in kldata csv")]
    EmptyRow,
    #[error("Failed to parse timestamp: {0}")]
    Chrono(#[from] chrono::ParseError),
    #[error("value {0} could not be parsed as float")]
    Float(String),
    #[error("unrecognised param_code '{0}'")]
    UnrecognisedParamCode(String),
}

/// FIXME: these params are scalar in Stinfosys, but are not when coming from Obsinn.
pub const SPECIAL_CASES: [&str; 6] = [
    // METAR params that come in as 'xxL' and 'xxR', where 'x' is a numeric character.
    // We need to decide how to treat them (Kvalobs silently discards them apparently)
    // Or if they need to be changed in Stinfosys
    "X1R", "X2R", "X3R", "WS", "WS2",
    // seems to be a number most of the times, but gets an 'a' every once in a while.
    // Maybe it's in hex format?
    "W1",
    // Leaving the following disabled, since even though I've seen them with these weird
    // outputs, they do seem to primarily send float data. These weird values seem to be
    // their elementids?
    // with value "Flatetemperatur"
    //"TSS",
    // with value "windspeed10m"
    //"FF_01",
    // with value "winddir10m"
    //"DD_01",
];

/// Represents a set of observations that came in the same message from obsinn, with shared
/// station_id and type_id
#[derive(Debug, PartialEq)]
pub struct ObsinnChunk {
    observations: Vec<ObsinnObs>,
    station_id: i32, // TODO: change name here to nationalnummer?
    type_id: i32,
    timestamp: DateTime<Utc>,
}

/// Represents a single observation from an obsinn message
#[derive(Debug, PartialEq)]
pub struct ObsinnObs {
    id: ObsinnId,
    value: ObsType,
}

/// Identifier for a single observation within a given obsinn message
#[derive(Debug, Clone, PartialEq)]
pub struct ObsinnId {
    pub param_code: String,
    pub sensor_and_level: Option<(i32, i32)>,
}

// TODO: maybe this can be a field in ObsinnChunk?
pub struct ObsinnHeader {
    pub station_id: i32,
    pub type_id: i32,
    pub message_id: usize,
    // There is an optional field with the timestamp when the data in the message was received by
    // Obsinn, which we don't currently parse, since we have no use for it
}

impl ObsinnHeader {
    pub fn parse(meta: &str) -> Result<Self, ParseError> {
        let mut fields = meta.split('/');

        let kldata_string = fields.next().ok_or(ParseError::HeaderTermination)?;

        if kldata_string != "kldata" {
            return Err(ParseError::IndicatorMissing);
        }

        let unexpected_field_error = |field: &str| ParseError::UnexpectedField(field.to_string());

        let mut station_id: Option<i32> = None;
        let mut type_id: Option<i32> = None;
        let mut message_id: Option<usize> = None;

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
                "received_time" => (),
                _ => return Err(unexpected_field_error(field)),
            }
        }

        Ok(ObsinnHeader {
            station_id: station_id
                .ok_or_else(|| ParseError::MissingField("nationalnr".to_string()))?,
            type_id: type_id.ok_or_else(|| ParseError::MissingField("type".to_string()))?,
            message_id: message_id.unwrap_or(0),
        })
    }
}

fn parse_value<T: FromStr>(key: &str, value: &str) -> Result<T, ParseError>
where
    <T as FromStr>::Err: Debug,
{
    value
        .parse::<T>()
        .map_err(|_| ParseError::InvalidValue(value.to_string(), key.to_string()))
}

pub fn parse_columns(cols_raw: &str) -> Result<Vec<ObsinnId>, ParseError> {
    // this regex is taken from ODA kvkafka's kldata parser
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
        .collect::<Result<Vec<ObsinnId>, ParseError>>()
}

pub fn parse_scalar(val: &str, col: &ObsinnId) -> ObsType {
    // NOTE(1): some params can be empty (old formats that were carried over
    // or a hacky way to have the observations deleted)
    // NOTE(2): some params can be simply "-" instead of being empty (hack?
    // Does it have a meaning?)
    // NOTE(3): Søren told us to treat "///", which we often see on some common
    // scalar params like "TA", the same as "-"
    if val.is_empty() || val == "-" || val == "///" {
        return ObsType::Scalar(None);
    }

    match val.parse::<f64>() {
        Ok(parsed) => {
            if parsed == -32766.0 || parsed == -32767.0 {
                // these are null values sent by kvalobs,
                // they basically are saying data was expected but didn't come
                // we replace with nulls
                ObsType::Scalar(None)
            } else {
                ObsType::Scalar(Some(parsed))
            }
        }
        // cases we haven't encountered yet, that also turn out to not be parseable as floats
        Err(_) => {
            debug!(
                "Param \"{}\" with value \"{}\" marked as scalar in stinfosys could not be \
                    parsed as float. Consider adding it to SPECIAL_CASES",
                col.param_code, val
            );
            ObsType::NonScalar(Some(val.to_string()))
        }
    }
}

pub fn parse_nonscalar(val: &str) -> ObsType {
    if val.is_empty() || val == "-" || val == "///" {
        return ObsType::NonScalar(None);
    }

    ObsType::NonScalar(Some(val.to_string()))
}

fn parse_obs(
    csv_body: Lines,
    columns: &[ObsinnId],
    reference_params: ParamConversions,
    header: ObsinnHeader,
) -> Result<Vec<ObsinnChunk>, ParseError> {
    let mut chunks = Vec::new();

    for row in csv_body {
        let mut obs = Vec::new();
        let (timestamp, vals) = {
            let mut vals = row.split(',').map(str::trim);

            let raw_timestamp = vals.next().ok_or(ParseError::EmptyRow)?;

            // TODO: timestamp parsing needs to handle milliseconds and truncated timestamps?
            let timestamp = NaiveDateTime::parse_from_str(raw_timestamp, "%Y%m%d%H%M%S")?.and_utc();

            (timestamp, vals)
        };

        // used to increment metrics
        let mut num_scalar = 0;
        let mut num_nonscalar = 0;

        for (i, val) in vals.enumerate() {
            // TODO: should we do some smart bounds-checking??
            let col = columns[i].clone();

            let value = match reference_params.get(&col.param_code) {
                // NOTE: we assume ref_params marked as scalar in Stinfosys to be floats (but
                // could be ints, which wouldn't be ideal?)
                Some(ref_param) => {
                    if ref_param.is_scalar && !SPECIAL_CASES.contains(&col.param_code.as_str()) {
                        num_scalar += 1;
                        parse_scalar(val, &col)
                    } else {
                        num_nonscalar += 1;
                        parse_nonscalar(val)
                    }
                }
                None => {
                    warn!("unrecognised param_code '{}': '{}'", col.param_code, val);
                    parse_nonscalar(val)
                }
            };

            obs.push(ObsinnObs { id: col, value })
        }

        metrics::counter!(SCALAR_DATAPOINTS).increment(num_scalar);
        metrics::counter!(NONSCALAR_DATAPOINTS).increment(num_nonscalar);

        // TODO: should this be more resiliant?
        if obs.is_empty() {
            return Err(ParseError::EmptyRow);
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
    reference_params: ParamConversions,
) -> Result<(usize, Vec<ObsinnChunk>), ParseError> {
    let (header, columns, csv_body) = {
        let mut csv_body = msg.lines();

        // parse the first two lines of the message as meta header, and csv column names,
        // leave the rest as an iter over the lines of csv body
        let header = ObsinnHeader::parse(csv_body.next().ok_or(ParseError::Lines)?)?;
        let columns = parse_columns(csv_body.next().ok_or(ParseError::Lines)?)?;

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
pub async fn filter_and_label_kldata(
    chunks: Vec<ObsinnChunk>,
    open_conn: &mut PooledPgConn<'_>,
    restricted_conn: &mut PooledPgConn<'_>,
    param_conversions: ParamConversions,
    permit_table: PermitTables,
    level_table: LevelTable,
) -> Result<(Vec<DataChunk>, Vec<DataChunk>), Error> {
    const QUERY_GET_OBSINN_STR: &str = r#"
        SELECT timeseries
            FROM labels.obsinn
            WHERE nationalnummer = $1
                AND type_id = $2
                AND param_code = $3
                AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
                AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))
        "#;
    let query_get_obsinn_open = open_conn.prepare(QUERY_GET_OBSINN_STR).await?;
    let query_get_obsinn_restricted = restricted_conn.prepare(QUERY_GET_OBSINN_STR).await?;

    let mut out_open_chunks = Vec::new();
    let mut out_restricted_chunks = Vec::new();
    for chunk in chunks {
        let mut open_data = Vec::new();
        let mut restricted_data = Vec::new();

        for in_datum in chunk.observations {
            let param_id = param_conversions
                .get(&in_datum.id.param_code)
                .map(|param| param.id);

            // TODO: With some changes to this function, we could potentially move its call outside
            // the loop body. For one thing, the station permit checks, if done in a separate
            // function, would apply to all observations. Since we know the param-specific permits
            // are barely used, we could also pre-emptively check all param permits outside the
            // loop.
            let permit = timeseries_get_permit(
                permit_table.clone(),
                chunk.station_id,
                chunk.type_id,
                param_id,
            )?;

            let (transaction, query_get_obsinn, data) = match permit {
                Some(1) => (
                    open_conn.transaction().await?,
                    &query_get_obsinn_open,
                    &mut open_data,
                ),
                _ => {
                    #[cfg(feature = "integration_tests")]
                    info!("station {}: timeseries is closed", chunk.station_id);
                    (
                        restricted_conn.transaction().await?,
                        &query_get_obsinn_restricted,
                        &mut restricted_data,
                    )
                }
            };

            let (sensor, lvl): (i32, i32) = in_datum.id.sensor_and_level.unwrap_or((0, 0));

            let obsinn_label_result = transaction
                .query_opt(
                    query_get_obsinn,
                    &[
                        &chunk.station_id,
                        &chunk.type_id,
                        &in_datum.id.param_code,
                        &lvl,
                        &sensor,
                    ],
                )
                .await?;

            // convert the level
            let level = if let Some(param_id) = param_id {
                param_get_level(level_table.clone(), param_id, lvl)?
            } else {
                None
            };

            let timeseries_id: i64 = match obsinn_label_result {
                Some(row) => row.get(0),
                None => {
                    // create new timeseries
                    // TODO: currently we create a timeseries with null location
                    // In the future the location column should be moved to the timeseries metadata table
                    let timeseries_id = transaction
                        .query_one(
                            "INSERT INTO public.timeseries (fromtime, permit) VALUES ($1, $2) RETURNING id",
                            &[&chunk.timestamp, &permit],
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
                    // use the adjusted level here, to remove the 0 = default hack at this level
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
                                &level,
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
        if !open_data.is_empty() {
            out_open_chunks.push(DataChunk {
                timestamp: chunk.timestamp,
                // TODO: real time_resolution (derive from type_id for now)
                time_resolution: type_id_to_time_resolution(chunk.type_id),
                data: open_data,
            });
        }
        if !restricted_data.is_empty() {
            out_restricted_chunks.push(DataChunk {
                timestamp: chunk.timestamp,
                // TODO: real time_resolution (derive from type_id for now)
                time_resolution: type_id_to_time_resolution(chunk.type_id),
                data: restricted_data,
            });
        }
    }

    Ok((out_open_chunks, out_restricted_chunks))
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
                Err(ParseError::IndicatorMissing),
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
                Err(ParseError::UnexpectedField("unexpected".to_string())),
                "unexpected field",
            ),
            (
                "kldata/messageid=10/type=501",
                Err(ParseError::MissingField("nationalnr".to_string())),
                "missing nationlnr",
            ),
            (
                "kldata/messageid=10/nationalnr=93140",
                Err(ParseError::MissingField("type".to_string())),
                "missing type",
            ),
        ];
        for (msg, expected, case_description) in cases {
            let output = ObsinnHeader::parse(msg)
                .map(|header| (header.station_id, header.type_id, header.message_id));
            assert_eq!(output, expected, "{case_description}");
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
            assert_eq!(output, expected, "{case_description}");
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
                },
                Ok(vec![ObsinnChunk {
                    observations: vec![
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "KLOBS".to_string(),
                                sensor_and_level: None,
                            },
                            value: NonScalar(Some("20240910000000".to_string())),
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
                },
                Ok(vec![ObsinnChunk {
                    observations: vec![
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "unknown".to_string(),
                                sensor_and_level: None,
                            },
                            value: NonScalar(Some("20240910000000".to_string())),
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
                            value: NonScalar(Some("24R".to_string())),
                        },
                        ObsinnObs {
                            id: ObsinnId {
                                param_code: "X2R".to_string(),
                                sensor_and_level: None,
                            },
                            value: NonScalar(Some("24L".to_string())),
                        },
                    ],
                    timestamp: Utc.with_ymd_and_hms(2024, 9, 10, 0, 0, 0).unwrap(),
                    station_id: 18700,
                    type_id: 511,
                }]),
                "special cases",
            ),
        ];

        let param_conversions = get_conversions("../resources/paramconversions.csv").unwrap();
        for (data, cols, header, expected, case_description) in cases {
            let output = parse_obs(data.lines(), &cols, param_conversions.clone(), header);
            assert_eq!(output, expected, "{case_description}");
        }
    }

    // NOTE: just test for basic failures, the happy path should already be captured by the other tests
    #[test]
    fn test_parse_kldata() {
        let cases = vec![
            ("", Err(ParseError::Lines), "empty line"),
            (
                "kldata/nationalnr=99993/type=508/messageid=23",
                Err(ParseError::Lines),
                "header only",
            ),
        ];
        let param_conversions = get_conversions("../resources/paramconversions.csv").unwrap();

        for (body, expected, case_description) in cases {
            let output = parse_kldata(body, param_conversions.clone());
            assert_eq!(output, expected, "{case_description}");
        }
    }
}
