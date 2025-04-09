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
    DbPools, PooledPgConn, KAFKA_FAILURES, KAFKA_MESSAGES_RECEIVED,
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

mod quality_code;
use quality_code::get_quality_code;

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

fn parse_message(xmlmsg: String) -> Result<Vec<RawDatum>, Error> {
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
    open_conn: &mut PooledPgConn<'_>,
    restricted_conn: &mut PooledPgConn<'_>,
    raw_data: Vec<RawDatum>,
    permit_table: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<(Vec<Datum>, Vec<Datum>), Error> {
    // TODO: should'nt we give this a source-specific label?
    const QUERY_GET_MET_STR: &str = r#"
        SELECT timeseries FROM labels.met
            WHERE station_id = $1
            AND param_id = $2
            AND type_id = $3
            AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
            AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))
        "#;
    let query_met_open = open_conn.prepare(QUERY_GET_MET_STR).await?;
    let query_met_restricted = restricted_conn.prepare(QUERY_GET_MET_STR).await?;

    let mut open_data: Vec<Datum> = Vec::new();
    let mut restricted_data: Vec<Datum> = Vec::new();

    // TODO: transforming this to an iterator would let us pipeline the queries
    for raw_datum in raw_data {
        let permit = timeseries_get_permit(
            permit_table.clone(),
            raw_datum.kvid.station,
            raw_datum.kvid.typeid,
            raw_datum.kvid.paramid,
        )?;

        let (transaction, query_met, data) = match permit {
            Some(1) => (
                open_conn.transaction().await?,
                &query_met_open,
                &mut open_data,
            ),
            _ => (
                restricted_conn.transaction().await?,
                &query_met_restricted,
                &mut restricted_data,
            ),
        };

        // let transaction = conn.transaction().await?;

        let tsid: i64 = match transaction
            .query(
                query_met,
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
                        "INSERT INTO public.timeseries (fromtime, permit) VALUES ($1, $2) RETURNING id",
                        &[&raw_datum.obstime, &permit],
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

    Ok((open_data, restricted_data))
}

async fn insert_kvdata(conn: &mut PooledPgConn<'_>, data: Vec<Datum>) -> Result<(), Error> {
    const QUERY_STR: &str = r#"
        INSERT INTO legacy.data
            (timeseries, obstime, corrected, quality_code, controlinfo, useinfo, cfailed)
        VALUES($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT ON CONSTRAINT unique_data_timeseries_obstime
            DO UPDATE SET
                corrected = EXCLUDED.corrected,
                quality_code = EXCLUDED.quality_code,
                controlinfo = EXCLUDED.controlinfo,
                useinfo = EXCLUDED.useinfo,
                cfailed = EXCLUDED.cfailed
            "#;
    let query = conn.prepare(QUERY_STR).await?;

    let mut futures = data
        .iter()
        .map(|datum| async {
            let quality_code = datum.kvdata.useinfo.as_ref().map(|f| get_quality_code(f));

            conn.execute(
                &query,
                &[
                    &datum.tsid,
                    &datum.obstime,
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

    Ok(())
}

pub async fn ingest_kvkafka(
    pools: DbPools,
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

    // Channel buffer size here is based on pure vibes, feel free to change it
    let (parse_tx, mut parse_rx) = tokio::sync::mpsc::channel::<String>(1);
    let (db_tx, mut db_rx) = tokio::sync::mpsc::channel::<Vec<RawDatum>>(100);

    // Needs to be on a sync thread because processing a message is sync and I measured it to take
    // ~200us on average. Tokio tasks should not go more than 10-100us between await points
    // according to tokio devs to avoid choking the runtime. See:
    // https://ryhl.io/blog/async-what-is-blocking/
    let _parse_thread = std::thread::spawn(move || {
        while let Some(message) = parse_rx.blocking_recv() {
            // FIXME: handle errors
            let raw_data = parse_message(message).unwrap();
            db_tx.blocking_send(raw_data).unwrap();
        }
    });
    let db_task = tokio::task::spawn(async move {
        // FIXME: handle errors
        let mut open_conn = pools.open.get().await.unwrap();
        let mut restricted_conn = pools.restricted.get().await.unwrap();

        // TODO: use recv_many
        while let Some(raw_data) = db_rx.recv().await {
            // FIXME: handle errors
            let (open_data, restricted_data) = filter_and_label_kvdata(
                &mut open_conn,
                &mut restricted_conn,
                raw_data,
                permit_table.clone(),
            )
            .await
            .unwrap();

            insert_kvdata(&mut open_conn, open_data).await.unwrap();
            insert_kvdata(&mut restricted_conn, restricted_data)
                .await
                .unwrap();
        }
    });

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                eprintln!("cancellation token triggered");
                // This will cause the parse thread to break and return, dropping db_tx,
                // which will in turn cause db_task to break and return
                drop(parse_tx);
                break;
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
                                let message = payload_str.trim().replace(['\n', '\\'], "");

                                if let Err(e) = parse_tx.send(message).await {
                                    metrics::counter!(KAFKA_FAILURES).increment(1);
                                    error!("failed to send kafka message for parsing: {}, payload: {}", e, payload_str);
                                }
                            },
                            Some(Err(_)) => {
                                metrics::counter!(KAFKA_FAILURES).increment(1);
                                error!("failed to parse kafka payload as utf8. payload: {:?}",  message.payload());
                            },
                            None => warn!("Received empty message from kafka"),
                        }

                        // TODO: move to db thread
                        if let Err(e) = consumer.store_offset_from_message(&message) {
                            metrics::counter!(KAFKA_FAILURES).increment(1);
                            error!("failed to mark offset: {}", e);
                        }
                    }
                }
            }
        }
    }

    // Wait for message processing to finish before exiting
    // FIXME: handle error
    db_task.await.unwrap();

    Ok(())
}
