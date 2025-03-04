use chrono::{DateTime, NaiveDateTime, Utc};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{PgConnectionPool, KAFKA_FAILURES, KAFKA_MESSAGES_RECEIVED};

#[derive(Error, Debug)]
pub enum Error {
    #[error("parsing xml error: {0}")]
    IssueParsingXML(String),
    #[error("parsing time error: {0}")]
    IssueParsingTime(#[from] chrono::ParseError),
    #[error("kafka returned an error: {0}")]
    Kafka(#[from] kafka::Error),
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("error while deserializing message: {0}")]
    Deserialize(#[from] quick_xml::DeError),
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
pub struct Msg {
    kvid: KvalobsId,
    obstime: DateTime<Utc>,
    kvdata: Kvdata,
}

pub async fn read_and_insert(
    pool: PgConnectionPool,
    group_string: String,
    cancel_token: CancellationToken,
) {
    let (tx, mut rx) = mpsc::channel(10);

    tokio::spawn(async move {
        read_kafka(group_string, tx, cancel_token).await;
    });

    let mut client = pool.get().await.expect("couldn't connect to database");
    while let Some(msg) = rx.recv().await {
        if let Err(e) = insert_kvdata(&mut client, msg).await {
            metrics::counter!(KAFKA_FAILURES).increment(1);
            error!("Database insert error: {e}");
        }
    }
}

pub async fn parse_message(message: &[u8], tx: &mpsc::Sender<Msg>) -> Result<(), Error> {
    // do some basic trimming / processing of the raw message
    // received from the kafka queue
    let xmlmsg = std::str::from_utf8(message)
        .map_err(|_| Error::IssueParsingXML("couldn't convert message from utf8".to_string()))?
        .trim()
        .replace(['\n', '\\'], "");

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
                                for data in kvdata {
                                    let msg = Msg {
                                        kvid: KvalobsId {
                                            station: station.val,
                                            paramid: data.paramid,
                                            typeid: typeid.val,
                                            sensor: sensor.val,
                                            level: level.val,
                                        },
                                        obstime: obs_time,
                                        kvdata: data,
                                    };
                                    tx.send(msg).await.unwrap();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

async fn read_kafka(group_name: String, tx: mpsc::Sender<Msg>, cancel_token: CancellationToken) {
    // NOTE: reading from the 4 redundant kafka queues, but only reading the checked data (other topics exists)
    let mut consumer = Consumer::from_hosts(vec![
        "kafka2-a1.met.no:9092".to_owned(),
        "kafka2-a2.met.no:9092".to_owned(),
        "kafka2-b1.met.no:9092".to_owned(),
        "kafka2-b2.met.no:9092".to_owned(),
    ])
    .with_topic_partitions("kvalobs.production.checked".to_owned(), &[0, 1])
    .with_fallback_offset(FetchOffset::Earliest)
    .with_group(group_name)
    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    .create()
    .expect("failed to create consumer");

    // Consume the kafka queue infinitely
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                eprintln!("cancellation token triggered");
                break;
            }
            // https://docs.rs/kafka/latest/src/kafka/consumer/mod.rs.html#155
            // poll asks for next available chunk of data as a MessageSet
            poll_result = async { consumer.poll() } => {
                match poll_result {
                    Ok(sets) => {
                        // used for metrics
                        let mut num_messages = 0;

                        for msgset in sets.iter() {
                            for msg in msgset.messages() {
                                num_messages += 1;
                                if let Err(e) = parse_message(msg.value, &tx).await {
                                    metrics::counter!(KAFKA_FAILURES).increment(1);
                                    error!("failed to parse kafka message: {}, msg.value: {:?}", e, msg.value);
                                }
                            }
                            if let Err(e) = consumer.consume_messageset(msgset) {
                                metrics::counter!(KAFKA_FAILURES).increment(1);
                                error!("failed to consume messageset: {}", e);
                            }
                        }

                        metrics::counter!(KAFKA_MESSAGES_RECEIVED).increment(num_messages);

                        consumer
                            .commit_consumed()
                            // FIXME: I wonder if an expect is too harsh here? we probably don't want to
                            // crash the task
                            .expect("could not commit offset in consumer"); // ensure we keep offset
                    }
                    Err(e) => {
                        metrics::counter!(KAFKA_FAILURES).increment(1);
                        eprintln!("failed to poll kafka: {}\nRetrying in 5 seconds...", Error::Kafka(e));
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }
}

async fn create_timeseries(
    timestamp: &DateTime<Utc>,
    kvid: &KvalobsId,
    transaction: tokio_postgres::Transaction<'_>,
) -> Result<i64, tokio_postgres::Error> {
    // create new timeseries
    // TODO: currently we create a timeseries with null location
    // In the future the location column should be moved to the timeseries metadata table
    let timeseries_id = transaction
        .query_one(
            "INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id",
            &[&timestamp],
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
                &kvid.station,
                &kvid.paramid,
                &kvid.typeid,
                &kvid.level,
                &kvid.sensor,
            ],
        )
        .await?;

    transaction.commit().await?;
    Ok(timeseries_id)
}

pub async fn insert_kvdata(
    client: &mut tokio_postgres::Client,
    Msg {
        kvid,
        obstime,
        kvdata,
    }: Msg,
) -> Result<(), Error> {
    // query timeseries ID
    // NOTE: alternately could use conn.query_one, since we want exactly one response
    let tsid: i64 = match client
        .query(
            "SELECT timeseries FROM labels.met
                WHERE station_id = $1
                AND param_id = $2
                AND type_id = $3
                AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
                AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))",
            &[
                &kvid.station,
                &kvid.paramid,
                &kvid.typeid,
                &kvid.level,
                &kvid.sensor,
            ],
        )
        .await?
        .first()
    {
        Some(row) => row.get(0),
        None => {
            let transaction = client.transaction().await?;
            create_timeseries(&obstime, &kvid, transaction).await?
        }
    };

    // write the data into the db
    client.execute(
        "INSERT INTO flags.kvdata (timeseries, obstime, original, corrected, controlinfo, useinfo, cfailed)
            VALUES($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT ON CONSTRAINT unique_kvdata_timeseries_obstime
                    DO UPDATE SET
                        original = EXCLUDED.original,
                        corrected = EXCLUDED.corrected,
                        controlinfo = EXCLUDED.controlinfo,
                        useinfo = EXCLUDED.useinfo,
                        cfailed = EXCLUDED.cfailed",
        &[&tsid, &obstime, &kvdata.original, &kvdata.corrected, &kvdata.controlinfo, &kvdata.useinfo, &kvdata.cfailed],
    ).await?;

    Ok(())
}
