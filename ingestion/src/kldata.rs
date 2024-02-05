use crate::{Datum, PooledPgConn};
use chrono::{DateTime, Utc};
use regex::Regex;
use std::{
    fmt::Debug,
    str::{FromStr, Lines},
};

// TODO: verify integer types
pub struct ObsinnObs {
    timestamp: DateTime<Utc>,
    id: ObsinnId,
    value: f32,
}

pub struct ObsinnChunk {
    observations: Vec<ObsinnObs>,
    station_id: i32, // TODO: change name here to nationalnummer?
    type_id: i32,
}

#[derive(Debug, Clone)]
struct ObsinnId {
    param_code: String,
    sensor_and_level: Option<(i32, i32)>,
}

fn parse_meta_field<T: FromStr>(
    field: &str,
    expected_key: &'static str,
) -> Result<T, Box<dyn std::error::Error>>
where
    <T as FromStr>::Err: Debug,
{
    let (key, value) = field.split_once('=').unwrap();

    assert_eq!(key, expected_key);

    Ok(value.parse::<T>().unwrap())
}

fn parse_meta(meta: &str) -> Result<(i32, i32, usize), &dyn std::error::Error> {
    let mut parts = meta.splitn(4, '/');

    assert_eq!(parts.next().unwrap(), "kldata");

    let station_id = parse_meta_field(parts.next().unwrap(), "nationalnr").unwrap();
    let type_id = parse_meta_field(parts.next().unwrap(), "type").unwrap();
    let message_id = parse_meta_field(parts.next().unwrap(), "messageid").unwrap();

    Ok((station_id, type_id, message_id))
}

fn parse_columns(cols_raw: &str) -> Result<Vec<ObsinnId>, &dyn std::error::Error> {
    // this regex is taken from kvkafka's kldata parser
    // let col_regex = Regex::new(r"([^(),]+)(\([0-9]+,[0-9]+\))?").unwrap();
    // it is modified below to capture sensor and level separately, while keeping
    // the block collectively optional
    let col_regex = Regex::new(r"([^(),]+)(?\(([0-9]+),([0-9]+)\))?").unwrap();

    Ok(col_regex
        .captures_iter(cols_raw)
        .map(|caps| match caps.len() {
            2 => ObsinnId {
                param_code: caps.get(1).unwrap().as_str().to_owned(),
                sensor_and_level: None,
            },
            4 => ObsinnId {
                param_code: caps.get(1).unwrap().as_str().to_owned(),
                sensor_and_level: Some((
                    caps.get(2).unwrap().as_str().parse().unwrap(),
                    caps.get(3).unwrap().as_str().parse().unwrap(),
                )),
            },
            _ => panic!(),
        })
        .collect())
}

fn parse_obs(
    csv_body: Lines<'_>,
    columns: &[ObsinnId],
) -> Result<Vec<ObsinnObs>, Box<dyn std::error::Error>> {
    let mut obs = Vec::new();

    for row in csv_body {
        let (timestamp, vals) = {
            let mut vals = row.split(',');
            // TODO: timestamp parsing needs to handle milliseconds and truncated timestamps
            let timestamp = DateTime::parse_from_str(vals.next().unwrap(), "%Y%m%d%H%M%S")
                .unwrap()
                .with_timezone(&Utc);

            (timestamp, vals)
        };

        for (i, val) in vals.enumerate() {
            let col = columns[i].clone(); // Should we do some smart bounds-checking??

            // TODO: parse differently based on param_code?

            obs.push(ObsinnObs {
                timestamp,
                id: col,
                value: val.parse().unwrap(),
            })
        }
    }

    Ok(obs)
}

pub fn parse_kldata(msg: &str) -> Result<(usize, ObsinnChunk), &dyn std::error::Error> {
    // parse the first two lines of the message as meta, and csv column names,
    // leave the rest as an iter over the lines of csv body
    let (station_id, type_id, message_id, columns, csv_body) = {
        let mut lines = msg.lines();

        let (station_id, type_id, message_id) = parse_meta(lines.next().unwrap()).unwrap();
        let columns = parse_columns(lines.next().unwrap()).unwrap();

        (station_id, type_id, message_id, columns, lines)
    };

    Ok((
        message_id,
        ObsinnChunk {
            observations: parse_obs(csv_body, &columns).unwrap(),
            station_id,
            type_id,
        },
    ))
}

// TODO: rewrite such that queries can be pipelined?
// not pipelining here hurts latency, but shouldn't matter for throughput
pub async fn label_kldata(
    chunk: ObsinnChunk,
    conn: &mut PooledPgConn<'_>,
) -> Result<Vec<Datum>, Box<dyn std::error::Error>> {
    let query_get_obsinn = conn
        .prepare(
            "SELECT timeseries \
                FROM labels.obsinn \
                WHERE nationalnummer = $1 \
                    AND type_id = $2 \
                    AND param_code = $3 \
                    AND lvl = $4 \
                    AND sensor = $5",
        )
        .await
        .unwrap();

    let mut data = Vec::with_capacity(chunk.observations.len());

    for in_datum in chunk.observations {
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
                let timeseries_id = transaction
                    .query_one(
                        "INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id",
                        &[&in_datum.timestamp],
                    )
                    .await
                    .unwrap()
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
                    .await
                    .unwrap();

                // create filter label
                transaction
                    .execute(
                        "INSERT INTO labels.filter \
                                (timeseries, station_id, element_id, lvl, sensor) \
                            VALUES ($1, $2, $3, $4, $5)",
                        &[
                            &timeseries_id,
                            &chunk.station_id,
                            &"FIXME".to_string(), // TODO: load actual element id
                            &lvl,
                            &sensor,
                        ],
                    )
                    .await
                    .unwrap();

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
