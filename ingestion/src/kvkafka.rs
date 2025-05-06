use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};
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
use tracing::{error, info, warn};

use crate::{
    permissions::{self, timeseries_get_permit, ParamPermitTable, StationPermitTable},
    DbPools, PooledPgConn, KAFKA_FAILURES, KAFKA_MESSAGES_RECEIVED,
};

// The number of parsed kafka messages that can build up waiting for the DB task
const DB_BUFFER_SIZE: usize = 200;

// Query to get a tsid from the relevant source-specific label
const QUERY_GET_MET_STR: &str = r#"
    SELECT timeseries FROM labels.kvalobs
        WHERE station_id = $1
        AND param_id = $2
        AND type_id = $3
        AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
        AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))
    "#;

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

mod quality_code;
pub use quality_code::get_quality_code;

#[derive(Debug, Clone, Deserialize)]
struct KvalobsId {
    station: i32,
    paramid: i32,
    typeid: i32,
    sensor: Option<i32>,
    level: Option<i32>,
}

#[derive(Debug, Clone)]
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

    // Documentation on the available config options can be found at
    // https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
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
        // if we don't have a starting offset, or it's out of range, start from the earliest
        // available on the cluster
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Warning)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topic");

    consumer
}

fn parse_message(xmlmsg: &str) -> Result<Vec<RawDatum>, Error> {
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

async fn create_timeseries(
    conn: &mut PooledPgConn<'_>,
    raw_datum: &RawDatum,
    permit: Option<i32>,
) -> Result<i64, Error> {
    let transaction = conn.transaction().await?;

    // lock timseries table so we don't risk duplicate timeseries creation
    //
    // SHARE ROW EXCLUSIVE is chosen because:
    // - it conflicts with itself, so only one of these transactions can run at a time
    // - it does not conflict with ROW SHARE, so SELECTs outside transactions (the happy path of
    //   ingestion, plus egress) can still run.
    //
    // INSERT already acquires SHARE ROW EXCLUSIVE, but the explicit lock here is to make sure it
    // covers the SELECT that checks for an existing label too.
    //
    // We only need to lock public.timeseries and not the labels because the labels exist to
    // describe a timeseries. They should always be there if the timeseries exists, and if it
    // doesn't (i.e the public.timeseries INSERT fails), the transaction will be rolled back.
    //
    // The lock does not need to be explicitly released (in fact there is no way to do that), in
    // postgres locks are tied to transactions and are released when the transaction is committed
    // or rolled back.
    transaction
        .execute(
            "LOCK TABLE public.timeseries IN SHARE ROW EXCLUSIVE MODE",
            &[],
        )
        .await?;

    // re-check for an existing label since the first check was outside the transaction
    let rows = transaction
        .query(
            QUERY_GET_MET_STR,
            &[
                &raw_datum.kvid.station,
                &raw_datum.kvid.paramid,
                &raw_datum.kvid.typeid,
                &raw_datum.kvid.level,
                &raw_datum.kvid.sensor,
            ],
        )
        .await?;
    if let Some(row) = rows.first() {
        return Ok(row.get(0));
    }

    // TODO: currently we create a timeseries with null location
    // In the future the location column should be moved to the timeseries metadata table
    let timeseries_id = transaction
        .query_one(
            "INSERT INTO public.timeseries (fromtime, permit) VALUES ($1, $2) RETURNING id",
            &[&raw_datum.obstime, &permit],
        )
        .await?
        .get(0);

    // create source-specific label
    transaction
        .execute(
            "INSERT INTO labels.kvalobs \
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

    Ok(timeseries_id)
}

async fn label_kvdata(
    conn: &mut PooledPgConn<'_>,
    raw: Vec<(RawDatum, Option<i32>)>,
    query_met: tokio_postgres::Statement,
) -> Result<Vec<Datum>, Error> {
    let mut fails: Vec<usize> = Vec::new();
    let mut data: Vec<Datum> = Vec::new();

    let mut futures = raw
        .iter()
        .map(|(raw_datum, _)| async {
            conn.query(
                &query_met,
                &[
                    &raw_datum.kvid.station,
                    &raw_datum.kvid.paramid,
                    &raw_datum.kvid.typeid,
                    &raw_datum.kvid.level,
                    &raw_datum.kvid.sensor,
                ],
            )
            .await
        })
        .collect::<FuturesOrdered<_>>()
        .enumerate();

    while let Some((i, res)) = futures.next().await {
        if let Some(row) = res?.first() {
            let tsid = row.get(0);
            data.push(Datum {
                tsid,
                obstime: raw[i].0.obstime,
                //this clone (╥﹏╥)
                kvdata: raw[i].0.kvdata.clone(),
            });
        } else {
            fails.push(i);
        }
    }
    // explicit drop is needed to free the borrow of the conn object, so we can use it mutably to
    // create missing timeseries
    drop(futures);

    for i in fails {
        let tsid = create_timeseries(conn, &raw[i].0, raw[i].1).await?;
        data.push(Datum {
            tsid,
            obstime: raw[i].0.obstime,
            kvdata: raw[i].0.kvdata.clone(),
        });
    }

    Ok(data)
}

async fn filter_and_label_kvdata(
    open_conn: &mut PooledPgConn<'_>,
    restricted_conn: &mut PooledPgConn<'_>,
    raw_data: &mut [(Vec<RawDatum>, (i32, i64))],
    permit_table: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<(Vec<Datum>, Vec<Datum>), Error> {
    let query_met_open = open_conn.prepare(QUERY_GET_MET_STR).await?;
    let query_met_restricted = restricted_conn.prepare(QUERY_GET_MET_STR).await?;

    let mut open_raw: Vec<(RawDatum, Option<i32>)> = Vec::new();
    let mut restricted_raw: Vec<(RawDatum, Option<i32>)> = Vec::new();

    for (raw_data_vec, _) in raw_data {
        for raw_datum in raw_data_vec {
            let permit = timeseries_get_permit(
                permit_table.clone(),
                raw_datum.kvid.station,
                raw_datum.kvid.typeid,
                raw_datum.kvid.paramid,
            )?;

            let dest = match permit {
                Some(1) => &mut open_raw,
                _ => &mut restricted_raw,
            };
            dest.push((raw_datum.clone(), permit));
        }
    }

    let (open_data, restricted_data) = tokio::join!(
        label_kvdata(open_conn, open_raw, query_met_open),
        label_kvdata(restricted_conn, restricted_raw, query_met_restricted)
    );

    Ok((open_data?, restricted_data?))
}

async fn insert_kvdata(conn: &mut PooledPgConn<'_>, data: Vec<Datum>) -> Result<(), Error> {
    const QUERY_STR: &str = r#"
        INSERT INTO legacy.data
            (timeseries, obstime, original, corrected, quality_code, controlinfo, useinfo, cfailed)
        VALUES($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT ON CONSTRAINT unique_data_timeseries_obstime
            DO UPDATE SET
                original = EXCLUDED.original,
                corrected = EXCLUDED.corrected,
                quality_code = EXCLUDED.quality_code,
                controlinfo = EXCLUDED.controlinfo,
                useinfo = EXCLUDED.useinfo,
                cfailed = EXCLUDED.cfailed
            "#;
    let query = conn.prepare(QUERY_STR).await?;

    let transaction = conn.transaction().await?;

    let mut futures = data
        .iter()
        .map(|datum| async {
            let quality_code = datum.kvdata.useinfo.as_ref().map(|f| get_quality_code(f));

            transaction
                .execute(
                    &query,
                    &[
                        &datum.tsid,
                        &datum.obstime,
                        &datum.kvdata.original,
                        &datum.kvdata.corrected,
                        &quality_code,
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
    drop(futures);

    transaction.commit().await?;

    Ok(())
}

async fn insert_batch(
    open_conn: &mut PooledPgConn<'_>,
    restricted_conn: &mut PooledPgConn<'_>,
    raw_data: &mut [(Vec<RawDatum>, (i32, i64))],
    permit_table: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<(), Error> {
    let (open_data, restricted_data) =
        filter_and_label_kvdata(open_conn, restricted_conn, raw_data, permit_table).await?;

    let (res1, res2) = tokio::join!(
        insert_kvdata(open_conn, open_data),
        insert_kvdata(restricted_conn, restricted_data)
    );
    res1?;
    res2?;

    Ok(())
}

pub async fn ingest_kvkafka(
    pools: DbPools,
    brokers: &str,
    group: &str,
    topic: &str,
    cancel_token: CancellationToken,
    permit_table: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<(), Error> {
    // TODO: Louise directly specified topic partitions 0 and 1 to subscribe to. Was there a reason
    // for this? The kafka group coordinator should automatically assign partitions to consumers
    // such that the group covers all partitions, and we shouldn't have to worry about it. On that
    // note though, should be spawn a consumer task for each partition? It should increase our
    // throughput
    let consumer = create_consumer(brokers, group, topic);

    // Channel buffer size here is based on pure vibes, feel free to change it
    let (parse_tx, mut parse_rx) = tokio::sync::mpsc::channel::<(String, (i32, i64))>(1);
    let (db_tx, mut db_rx) =
        tokio::sync::mpsc::channel::<(Vec<RawDatum>, (i32, i64))>(DB_BUFFER_SIZE);
    let (offset_tx, mut offset_rx) = tokio::sync::mpsc::channel::<(i32, i64)>(1);

    // Needs to be on a sync thread because processing a message is sync and I measured it to take
    // ~200us on average. Tokio tasks should not go more than 10-100us between await points
    // according to tokio devs to avoid choking the runtime. See:
    // https://ryhl.io/blog/async-what-is-blocking/
    let _parse_thread = std::thread::spawn(move || {
        while let Some((message, offset)) = parse_rx.blocking_recv() {
            let raw_data = match parse_message(&message) {
                Ok(raw_data) => raw_data,
                Err(e) => {
                    metrics::counter!(KAFKA_FAILURES).increment(1);
                    error!("Failed to parse kafka message: {}, message: {}", e, message,);
                    continue;
                }
            };
            if let Err(e) = db_tx.blocking_send((raw_data, offset)) {
                metrics::counter!(KAFKA_FAILURES).increment(1);
                error!("Failed to send parsed kafka message to db task: {}", e);
                break;
            };
        }
    });
    let db_task = tokio::task::spawn(async move {
        let mut open_conn = pools
            .open
            .get()
            .await
            .expect("Kvkafka DB task could'nt connect to open DB");
        let mut restricted_conn = pools
            .restricted
            .get()
            .await
            .expect("Kvkafka DB task could'nt connect to restricted DB");

        let mut data_buffer: Vec<(Vec<RawDatum>, (i32, i64))> = Vec::with_capacity(DB_BUFFER_SIZE);

        while db_rx.recv_many(&mut data_buffer, DB_BUFFER_SIZE).await != 0 {
            let (partition, offset) = data_buffer.last().unwrap().1;

            if let Err(e) = insert_batch(
                &mut open_conn,
                &mut restricted_conn,
                &mut data_buffer,
                permit_table.clone(),
            )
            .await
            {
                metrics::counter!(KAFKA_FAILURES).increment(1);
                error!(
                    "Failed to insert kafka messages: {}, partition&offset: {}&{}",
                    e, partition, offset
                );
                continue;
            };

            if let Err(e) = offset_tx.send((partition, offset)).await {
                metrics::counter!(KAFKA_FAILURES).increment(1);
                error!("Failed to send offset: {}", e);
            };
            data_buffer.clear();
        }
    });

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("Cancellation token triggered");
                // This will cause the parse thread to break and return, dropping db_tx,
                // which will in turn cause db_task to break and return
                drop(parse_tx);
                break;
            }
            Some((partition, offset)) = offset_rx.recv() => {
                if let Err(e) = consumer.store_offset(topic, partition, offset) {
                    metrics::counter!(KAFKA_FAILURES).increment(1);
                    error!("failed to mark offset: {}", e);
                }
            }
            poll_result = consumer.recv() => {
                match poll_result {
                    Err(e) => {
                        metrics::counter!(KAFKA_FAILURES).increment(1);
                        error!("failed to poll kafka: {}", Error::Kafka(e));
                    }
                    Ok(message) => {
                        metrics::counter!(KAFKA_MESSAGES_RECEIVED).increment(1);

                        match message.payload().map(std::str::from_utf8) {
                            Some(Ok(payload_str)) => {
                                // do some basic trimming / processing of the raw message
                                // received from the kafka queue
                                let message_xml = payload_str.trim().replace(['\n', '\\'], "");

                                if let Err(e) = parse_tx.send((message_xml, (message.partition(), message.offset()))).await {
                                    metrics::counter!(KAFKA_FAILURES).increment(1);
                                    error!("failed to send kafka message for parsing: {}, payload: {}", e, payload_str);
                                    break;
                                }
                            },
                            Some(Err(_)) => {
                                metrics::counter!(KAFKA_FAILURES).increment(1);
                                error!("failed to parse kafka payload as utf8. payload: {:?}",  message.payload());
                            },
                            None => warn!("Received empty message from kafka"),
                        }

                    }
                }
            }
        }
    }

    while let Some((partition, offset)) = offset_rx.recv().await {
        if let Err(e) = consumer.store_offset(topic, partition, offset) {
            metrics::counter!(KAFKA_FAILURES).increment(1);
            error!("failed to mark offset: {}", e);
        }
    }

    // Wait for message processing to finish before exiting
    if let Err(e) = db_task.await {
        error!("Failed to join kvkafka DB task: {}", e);
    }

    info!("Kvkafka ingestion task finished");

    Ok(())
}
