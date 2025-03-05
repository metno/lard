use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{stream::FuturesUnordered, StreamExt};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, ConsumerContext, StreamConsumer},
    error::{KafkaError, KafkaResult},
    ClientConfig, ClientContext, Message, TopicPartitionList,
};
use serde::Deserialize;
use std::sync::{Arc, RwLock};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::{
    permissions::{self, timeseries_get_permit, ParamPermitTable, StationPermitTable},
    PgConnectionPool, PooledPgConn, KAFKA_FAILURES, KAFKA_MESSAGES_RECEIVED,
};

#[derive(Error, Debug)]
pub enum Error {
    #[error("parsing xml error: {0}")]
    IssueParsingXML(String),
    #[error("parsing time error: {0}")]
    IssueParsingTime(#[from] chrono::ParseError),
    #[error("kafka returned an error: {0}")]
    Kafka(#[from] KafkaError),
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool could not return a connection: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),
    #[error("error while deserializing message: {0}")]
    Deserialize(#[from] quick_xml::DeError),
    #[error("error handling permits: {0}")]
    Permissions(#[from] permissions::Error),
}

mod xml_types;
use xml_types::{KvalobsData, Kvdata};

#[derive(Debug, Deserialize)]
struct KvalobsId {
    station: i32,
    paramid: i32,
    typeid: i32,
    sensor: Option<i32>,
    level: Option<i32>,
}

#[derive(Debug)]
pub struct RawDatum {
    kvid: KvalobsId,
    obstime: DateTime<Utc>,
    kvdata: Kvdata,
}

#[derive(Debug)]
struct Datum {
    tsid: i64,
    obstime: DateTime<Utc>,
    kvdata: Kvdata,
}

// A simple context to customize the consumer behavior and log when commits fail
struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(_) => (),
            Err(e) => error!("Error while committing offsets: {}", e),
        };
    }
}

// Define a new type for convenience
type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

fn create_consumer(brokers: &str, group_id: &str, topic: &str) -> LoggingConsumer {
    let context = LoggingConsumerContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        .set_log_level(RDKafkaLogLevel::Warning)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topic");

    consumer
}

// TODO: investigate if we should put this on a blocking task
fn parse_message(message: &str) -> Result<Vec<RawDatum>, Error> {
    // do some basic trimming / processing of the raw message
    // received from the kafka queue
    let xmlmsg = message.trim().replace(['\n', '\\'], "");

    // do some checking / further processing of message
    if !xmlmsg.starts_with("<?xml") {
        return Err(Error::IssueParsingXML(
            "kv2kvdata must be xml starting with '<?xml'".to_string(),
        ));
    }

    let xmlmsg = match xmlmsg.find("?>") {
        Some(loc) => &xmlmsg[(loc + 2)..],
        None => {
            return Err(Error::IssueParsingXML(
                "couldn't find end of xml tag '?>'".to_string(),
            ))
        }
    };
    let item: KvalobsData = quick_xml::de::from_str(xmlmsg)?;

    let mut data: Vec<RawDatum> = Vec::new();

    // get the useful stuff out of this struct
    for station in item.stations {
        for typeid in station.typeids {
            for obstime in typeid.obstimes {
                let obs_time =
                    match NaiveDateTime::parse_from_str(&obstime.val, "%Y-%m-%d %H:%M:%S") {
                        Ok(time) => time.and_utc(),
                        Err(e) => {
                            metrics::counter!(KAFKA_FAILURES).increment(1);
                            error!(
                                "time parsing failed in kafka message: {}, original: {}",
                                Error::IssueParsingTime(e),
                                &obstime.val
                            );
                            continue;
                        }
                    };
                for tbtime in obstime.tbtimes {
                    // NOTE: tbtime is "table time" which can vary from the actual observation time,
                    // it's the first time it entered the db in kvalobs. Currently not using it
                    // TODO: Do we want to handle text data at all? It doesn't seem to be QCed
                    // if let Some(textdata) = tbtime.kvtextdata {...}
                    for sensor in tbtime.sensors {
                        for level in sensor.levels {
                            if let Some(kvdata) = level.kvdata {
                                for kvdatum in kvdata {
                                    data.push(RawDatum {
                                        kvid: KvalobsId {
                                            station: station.val,
                                            paramid: kvdatum.paramid,
                                            typeid: typeid.val,
                                            sensor: sensor.val,
                                            level: level.val,
                                        },
                                        obstime: obs_time,
                                        kvdata: kvdatum,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(data)
}

async fn filter_and_label_kvdata(
    conn: &mut PooledPgConn<'_>,
    raw_data: Vec<RawDatum>,
    permit_table: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<Vec<Datum>, Error> {
    // TODO: should'nt we give this a source-specific label?
    const QUERY_GET_MET_STR: &str = r#"
        SELECT timeseries FROM labels.met
            WHERE station_id = $1
            AND param_id = $2
            AND type_id = $3
            AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
            AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))
        "#;
    let query_get_met = conn.prepare(QUERY_GET_MET_STR).await?;

    let mut data: Vec<Datum> = Vec::new();

    // TODO: transforming this to an iterator would let us pipeline the queries
    for raw_datum in raw_data {
        let permit = timeseries_get_permit(
            permit_table.clone(),
            raw_datum.kvid.station,
            raw_datum.kvid.typeid,
            raw_datum.kvid.paramid,
        )?;
        if permit != Some(1) {
            continue;
        }

        let transaction = conn.transaction().await?;

        let tsid: i64 = match transaction
            .query(
                &query_get_met,
                &[
                    &raw_datum.kvid.station,
                    &raw_datum.kvid.paramid,
                    &raw_datum.kvid.typeid,
                    &raw_datum.kvid.level,
                    &raw_datum.kvid.sensor,
                ],
            )
            .await?
            .first()
        {
            Some(row) => row.get(0),
            _ => {
                // create new timeseries
                // TODO: currently we create a timeseries with null location
                // In the future the location column should be moved to the timeseries metadata table
                let timeseries_id = transaction
                    .query_one(
                        "INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id",
                        &[&raw_datum.obstime],
                    )
                    .await?
                    .get(0);

                // create met label
                transaction
                    .execute(
                        "INSERT INTO labels.met \
        (timeseries, station_id, param_id, type_id, lvl, sensor) \
    VALUES ($1, $2, $3, $4, $5, $6)",
                        &[
                            &timeseries_id,
                            &raw_datum.kvid.station,
                            &raw_datum.kvid.paramid,
                            &raw_datum.kvid.typeid,
                            &raw_datum.kvid.level,
                            &raw_datum.kvid.sensor,
                        ],
                    )
                    .await?;

                transaction.commit().await?;

                timeseries_id
            }
        };

        data.push(Datum {
            tsid,
            obstime: raw_datum.obstime,
            kvdata: raw_datum.kvdata,
        });
    }

    Ok(data)
}

async fn insert_kvdata(conn: &mut PooledPgConn<'_>, data: Vec<Datum>) -> Result<(), Error> {
    let query = conn
        .prepare(
            r#"
        INSERT INTO flags.kvdata
            (timeseries, obstime, original, corrected, controlinfo, useinfo, cfailed)
        VALUES($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT ON CONSTRAINT unique_kvdata_timeseries_obstime
            DO UPDATE SET
                original = EXCLUDED.original,
                corrected = EXCLUDED.corrected,
                controlinfo = EXCLUDED.controlinfo,
                useinfo = EXCLUDED.useinfo,
                cfailed = EXCLUDED.cfailed
            "#,
        )
        .await?;

    let mut futures = data
        .iter()
        .map(|datum| async {
            conn.execute(
                &query,
                &[
                    &datum.tsid,
                    &datum.obstime,
                    &datum.kvdata.original,
                    &datum.kvdata.corrected,
                    &datum.kvdata.controlinfo,
                    &datum.kvdata.useinfo,
                    &datum.kvdata.cfailed,
                ],
            )
            .await
        })
        .collect::<FuturesUnordered<_>>();

    while let Some(res) = futures.next().await {
        res?;
    }

    Ok(())
}

async fn process_message(
    conn: &mut PooledPgConn<'_>,
    message: &str,
    permit_table: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<(), Error> {
    let raw_data = parse_message(message)?;

    let data = filter_and_label_kvdata(conn, raw_data, permit_table).await?;

    insert_kvdata(conn, data).await
}

pub async fn ingest_kvkafka(
    pool: PgConnectionPool,
    group: String,
    cancel_token: CancellationToken,
    permit_table: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<(), Error> {
    const BROKERS: &str = "kafka2-a1.met.no:9092, kafka2-a2.met.no:9092, kafka2-b1.met.no:9092, kafka2-b2.met.no:9092";
    // TODO: Louise directly specified topic partitions 0 and 1 to subscribe to. Was there a reason
    // for this? The kafka group coordinator should automatically assign partitions to consumers
    // such that the group covers all partitions, and we shouldn't have to worry about it. On that
    // note though, should be spawn a consumer task for each partition? It should increase our
    // throughput
    const TOPIC: &str = "kvalobs.production.checked";
    let consumer = create_consumer(BROKERS, &group, TOPIC);

    let mut conn = pool.get().await?;

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                eprintln!("cancellation token triggered");
                break;
            }
            // consider batching or other StreamExt to optimise this
            poll_result = consumer.recv() => {
                match poll_result {
                    Err(e) => {
                        metrics::counter!(KAFKA_FAILURES).increment(1);
                        error!("failed to poll kafka: {}\nRetrying in 5 seconds...", Error::Kafka(e));
                    }
                    Ok(message) => {
                        metrics::counter!(KAFKA_MESSAGES_RECEIVED).increment(1);

                        match message.payload().map(std::str::from_utf8) {
                            Some(Ok(payload_str)) => {
                                if let Err(e) = process_message(&mut conn, payload_str, permit_table.clone()).await {
                                    metrics::counter!(KAFKA_FAILURES).increment(1);
                                    error!("failed to process kafka message: {}, payload: {}", e, payload_str);
                                }
                            },
                            Some(Err(_)) => {
                                metrics::counter!(KAFKA_FAILURES).increment(1);
                                error!("failed to parse kafka payload as utf8. payload: {:?}",  message.payload());
                            },
                            None => warn!("Received empty message from kafka"),
                        }

                        if let Err(e) = consumer.store_offset_from_message(&message) {
                            metrics::counter!(KAFKA_FAILURES).increment(1);
                            error!("failed to mark offset: {}", e);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
