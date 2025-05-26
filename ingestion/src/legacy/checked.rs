use chrono::NaiveDateTime;
use futures::{stream::FuturesUnordered, StreamExt};
use rdkafka::{consumer::Consumer, error::KafkaError, Message};
use thiserror::Error;
use tokio_postgres::Statement;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    legacy::common::{
        self, filter_and_label, Datum as CommonDatum, KvalobsId,
        UnlabelledDatum as CommonUnlabelledDatum,
    },
    levels::LevelTable,
    util::{
        kafka::{create_consumer, Offset},
        permissions::PermitTables,
        quality_code::get_quality_code,
        xml_types::{KvalobsData, Kvdata},
    },
    DbPools, PooledPgConn, KAFKA_CHECKED_FAILURES, KAFKA_CHECKED_MESSAGES_RECEIVED,
};

type Datum = CommonDatum<Kvdata>;
type UnlabelledDatum = CommonUnlabelledDatum<Kvdata>;

// The number of parsed kafka messages that can build up waiting for the DB task
const DB_BUFFER_SIZE: usize = 200;

const QUERY_STR: &str = r#"
    INSERT INTO legacy.data
        (timeseries, obstime, original, corrected, quality_code, controlinfo, useinfo, cfailed)
    VALUES($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT ON CONSTRAINT data_pkey
        DO UPDATE SET
            original = EXCLUDED.original,
            corrected = EXCLUDED.corrected,
            quality_code = EXCLUDED.quality_code,
            controlinfo = EXCLUDED.controlinfo,
            useinfo = EXCLUDED.useinfo,
            cfailed = EXCLUDED.cfailed
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
    #[error(transparent)]
    Common(#[from] common::Error),
}

type CheckedMsg = String;

fn parse_message(xmlmsg: &str) -> Result<Vec<UnlabelledDatum>, Error> {
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

    let mut data: Vec<UnlabelledDatum> = Vec::new();

    // get the useful stuff out of this struct
    for station in item.stations {
        for typeid in station.typeids {
            for obstime in typeid.obstimes {
                let obs_time =
                    match NaiveDateTime::parse_from_str(&obstime.val, "%Y-%m-%d %H:%M:%S") {
                        Ok(time) => time.and_utc(),
                        Err(e) => {
                            metrics::counter!(KAFKA_CHECKED_FAILURES).increment(1);
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
                                    data.push(UnlabelledDatum {
                                        kvid: KvalobsId {
                                            station: station.val,
                                            paramid: kvdatum.paramid,
                                            typeid: typeid.val,
                                            sensor: sensor.val.unwrap_or(0),
                                            level: level.val.unwrap_or(0),
                                        },
                                        obstime: obs_time,
                                        value: kvdatum,
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

async fn insert(
    conn: &mut PooledPgConn<'_>,
    data: Vec<Datum>,
    query: &Statement,
) -> Result<(), Error> {
    let transaction = conn.transaction().await?;

    let mut futures = data
        .iter()
        .map(|datum| async {
            let quality_code = datum.value.useinfo.as_ref().map(|f| get_quality_code(f));

            transaction
                .execute(
                    query,
                    &[
                        &datum.tsid,
                        &datum.obstime,
                        &datum.value.original,
                        &datum.value.corrected,
                        &quality_code,
                        &datum.value.controlinfo,
                        &datum.value.useinfo,
                        &datum.value.cfailed,
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
    raw_buffer: &[(Vec<UnlabelledDatum>, Offset)],
    permit_table: PermitTables,
    level_table: LevelTable,
    open_query: &Statement,
    restricted_query: &Statement,
) -> Result<(), Error> {
    let (open_data, restricted_data) = filter_and_label::<Kvdata>(
        open_conn,
        restricted_conn,
        raw_buffer,
        permit_table,
        level_table,
    )
    .await?;

    let (res1, res2) = tokio::join!(
        insert(open_conn, open_data, open_query),
        insert(restricted_conn, restricted_data, restricted_query)
    );
    res1?;
    res2?;

    Ok(())
}

pub async fn ingest(
    pools: DbPools,
    brokers: String,
    group: String,
    topic: &str,
    cancel_token: CancellationToken,
    permit_table: PermitTables,
    level_table: LevelTable,
) -> Result<(), Error> {
    // TODO: Louise directly specified topic partitions 0 and 1 to subscribe to. Was there a reason
    // for this? The kafka group coordinator should automatically assign partitions to consumers
    // such that the group covers all partitions, and we shouldn't have to worry about it. On that
    // note though, should be spawn a consumer task for each partition? It should increase our
    // throughput
    let consumer = create_consumer(brokers.as_str(), group.as_str(), topic);

    // Channel buffer size here is based on pure vibes, feel free to change it
    let (parse_tx, mut parse_rx) = tokio::sync::mpsc::channel::<(CheckedMsg, Offset)>(1);
    let (db_tx, mut db_rx) =
        tokio::sync::mpsc::channel::<(Vec<UnlabelledDatum>, Offset)>(DB_BUFFER_SIZE);
    let (offset_tx, mut offset_rx) = tokio::sync::mpsc::channel::<Offset>(1);

    // Needs to be on a sync thread because processing a message is sync and I measured it to take
    // ~200us on average. Tokio tasks should not go more than 10-100us between await points
    // according to tokio devs to avoid choking the runtime. See:
    // https://ryhl.io/blog/async-what-is-blocking/
    let _parse_thread = std::thread::spawn(move || {
        while let Some((message, offset)) = parse_rx.blocking_recv() {
            let raw_data = match parse_message(&message) {
                Ok(raw_data) => raw_data,
                Err(e) => {
                    metrics::counter!(KAFKA_CHECKED_FAILURES).increment(1);
                    error!("Failed to parse kafka message: {}, message: {}", e, message,);
                    continue;
                }
            };
            if let Err(e) = db_tx.blocking_send((raw_data, offset)) {
                metrics::counter!(KAFKA_CHECKED_FAILURES).increment(1);
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
            .expect("legacy::checked DB task could'nt connect to open DB");
        let mut restricted_conn = pools
            .restricted
            .get()
            .await
            .expect("legacy::checked DB task could'nt connect to restricted DB");

        let open_query = open_conn
            .prepare(QUERY_STR)
            .await
            .expect("legacy::checked DB task couldn't prepare open query");
        let restricted_query = restricted_conn
            .prepare(QUERY_STR)
            .await
            .expect("legacy::checked DB task couldn't prepare restricted query");

        let mut raw_buffer: Vec<(Vec<UnlabelledDatum>, Offset)> =
            Vec::with_capacity(DB_BUFFER_SIZE);

        while db_rx.recv_many(&mut raw_buffer, DB_BUFFER_SIZE).await != 0 {
            let offset = raw_buffer.last().unwrap().1.clone();

            if let Err(e) = insert_batch(
                &mut open_conn,
                &mut restricted_conn,
                &raw_buffer,
                permit_table.clone(),
                level_table.clone(),
                &open_query,
                &restricted_query,
            )
            .await
            {
                metrics::counter!(KAFKA_CHECKED_FAILURES).increment(1);
                error!(
                    "Failed to insert kafka messages: {}, offset: {:?}",
                    e, offset
                );
                continue;
            };

            if let Err(e) = offset_tx.send(offset).await {
                metrics::counter!(KAFKA_CHECKED_FAILURES).increment(1);
                error!("Failed to send offset: {}", e);
            };
            raw_buffer.clear();
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
            Some(Offset { partition, offset }) = offset_rx.recv() => {
                if let Err(e) = consumer.store_offset(topic, partition, offset) {
                    metrics::counter!(KAFKA_CHECKED_FAILURES).increment(1);
                    error!("failed to mark offset: {}", e);
                }
            }
            poll_result = consumer.recv() => {
                match poll_result {
                    Err(e) => {
                        metrics::counter!(KAFKA_CHECKED_FAILURES).increment(1);
                        error!("failed to poll kafka: {}", Error::Kafka(e));
                    }
                    Ok(message) => {
                        metrics::counter!(KAFKA_CHECKED_MESSAGES_RECEIVED).increment(1);

                        match message.payload().map(std::str::from_utf8) {
                            Some(Ok(payload_str)) => {
                                // do some basic trimming / processing of the raw message
                                // received from the kafka queue
                                let message_xml = payload_str.trim().replace(['\n', '\\'], "");

                                let offset = Offset { partition:message.partition(), offset: message.offset() };

                                if let Err(e) = parse_tx.send((message_xml, offset)).await {
                                    metrics::counter!(KAFKA_CHECKED_FAILURES).increment(1);
                                    error!("failed to send kafka message for parsing: {}, payload: {}", e, payload_str);
                                    break;
                                }
                            },
                            Some(Err(_)) => {
                                metrics::counter!(KAFKA_CHECKED_FAILURES).increment(1);
                                error!("failed to parse kafka payload as utf8. payload: {:?}",  message.payload());
                            },
                            None => warn!("Received empty message from kafka"),
                        }

                    }
                }
            }
        }
    }

    while let Some(Offset { partition, offset }) = offset_rx.recv().await {
        if let Err(e) = consumer.store_offset(topic, partition, offset) {
            metrics::counter!(KAFKA_CHECKED_FAILURES).increment(1);
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
