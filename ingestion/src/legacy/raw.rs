use chrono::NaiveDateTime;
use rdkafka::{consumer::Consumer, error::KafkaError, Message};
use std::str::Lines;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    kldata::{parse_columns, parse_kldata, ObsinnChunk, ObsinnHeader, ObsinnId, ParseError},
    legacy::common::{
        // Datum as CommonDatum,
        KvalobsId,
        RawDatum as CommonRawDatum,
    },
    util::{
        kafka::{create_consumer, Offset},
        levels::LevelTable,
        permissions::PermitTables,
    },
    DbPools, ParamConversions, KAFKA_RAW_FAILURES, KAFKA_RAW_MESSAGES_RECEIVED,
};

// The number of parsed kafka messages that can build up waiting for the DB task
const DB_BUFFER_SIZE: usize = 200;

#[derive(Error, Debug)]
pub enum Error {
    #[error("kafka returned an error: {0}")]
    Kafka(#[from] KafkaError),
    #[error("failed to parse kldata message: {0}")]
    Parse(#[from] ParseError),
}

// type Datum = CommonDatum<f64>;
type RawDatum = CommonRawDatum<f64>;

// modified version of kldata::parse_obs that returns RawDatum instead of ObsinnChunk
fn parse_obs(
    csv_body: Lines,
    columns: &[ObsinnId],
    reference_params: ParamConversions,
    header: ObsinnHeader,
) -> Result<Vec<RawDatum>, ParseError> {
    let mut obs = Vec::new();

    for row in csv_body {
        let (timestamp, vals) = {
            let mut vals = row.split(',');

            let raw_timestamp = vals.next().ok_or(ParseError::EmptyRow)?;

            // TODO: timestamp parsing needs to handle milliseconds and truncated timestamps?
            let timestamp = NaiveDateTime::parse_from_str(raw_timestamp, "%Y%m%d%H%M%S")?.and_utc();

            (timestamp, vals)
        };

        for (i, val) in vals.enumerate() {
            // TODO: should we do some smart bounds-checking??
            let col = columns[i].clone();

            // rejection is acceptable here, because things we don't catch should
            // be covered by the checked queue
            let paramid = reference_params
                .get(&col.param_code)
                .ok_or_else(|| ParseError::UnrecognisedParamCode(col.param_code.clone()))?
                .id;

            let (sensor, level) = col.sensor_and_level.unwrap_or((0, 0));

            let value: f64 = val
                .parse()
                .map_err(|_| ParseError::Float(val.to_string()))?;

            obs.push(RawDatum {
                kvid: KvalobsId {
                    station: header.station_id,
                    paramid,
                    typeid: header.type_id,
                    sensor,
                    level,
                },
                obstime: timestamp,
                value,
            })
        }
    }

    Ok(obs)
}

// modified version of kldata::parse_kldata that returns RawDatum instead of ObsinnChunk
pub fn parse(msg: &str, reference_params: ParamConversions) -> Result<Vec<RawDatum>, ParseError> {
    let (header, columns, csv_body) = {
        let mut csv_body = msg.lines();

        // parse the first two lines of the message as meta header, and csv column names,
        // leave the rest as an iter over the lines of csv body
        let header = ObsinnHeader::parse(csv_body.next().ok_or(ParseError::Lines)?)?;
        let columns = parse_columns(csv_body.next().ok_or(ParseError::Lines)?)?;

        (header, columns, csv_body)
    };

    parse_obs(csv_body, &columns, reference_params, header)
}

#[allow(clippy::too_many_arguments)]
pub async fn ingest(
    _pools: DbPools,
    brokers: String,
    group: String,
    topic: &'static str,
    cancel_token: CancellationToken,
    _permit_table: PermitTables,
    _level_table: LevelTable,
    param_conversions: ParamConversions,
) -> Result<(), Error> {
    let consumer = create_consumer(brokers.as_str(), group.as_str(), topic);

    let (db_tx, mut _db_rx) =
        tokio::sync::mpsc::channel::<(Vec<ObsinnChunk>, Offset)>(DB_BUFFER_SIZE);
    let (_offset_tx, mut offset_rx) = tokio::sync::mpsc::channel::<Offset>(1);

    let _db_task = tokio::task::spawn(async move {
        todo!();
    });

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("Cancellation token triggered");
                // This will cause db_task to break and return
                drop(db_tx);
                break;
            }
            Some(Offset { partition, offset }) = offset_rx.recv() => {
                if let Err(e) = consumer.store_offset(topic, partition, offset) {
                    metrics::counter!(KAFKA_RAW_FAILURES).increment(1);
                    error!("failed to mark offset on raw queue: {}", e);
                }
            }
            poll_result = consumer.recv() => {
                match poll_result {
                    Err(e) => {
                        metrics::counter!(KAFKA_RAW_FAILURES).increment(1);
                        error!("failed to poll raw kafka: {}", Error::Kafka(e));
                    }
                    Ok(message) => {
                        metrics::counter!(KAFKA_RAW_MESSAGES_RECEIVED).increment(1);

                        match message.payload().map(std::str::from_utf8) {
                            Some(Ok(payload_str)) => {
                                let offset = Offset { partition:message.partition(), offset: message.offset() };

                                // TODO: remove clone?
                                let (_, chunks) = parse_kldata(payload_str, param_conversions.clone())?;

                                db_tx.send((chunks, offset)).await.unwrap()
                            },
                            Some(Err(_)) => {
                                metrics::counter!(KAFKA_RAW_FAILURES).increment(1);
                                error!("failed to parse raw kafka payload as utf8. payload: {:?}",  message.payload());
                            },
                            None => warn!("Received empty message from raw kafka"),
                        }

                    }
                }
            }
        }
    }

    Ok(())
}
