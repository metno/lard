use chrono::{DateTime, NaiveDateTime, Utc};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use serde::{Deserialize, Deserializer};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::PgConnectionPool;

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
    #[error("no Timeseries ID found for this data - station {0}, param {1}")]
    TimeseriesMissing(i32, i32),
    #[error("error while deserializing message: {0}")]
    Deserialize(#[from] quick_xml::DeError),
}

#[derive(Debug, Deserialize)]
/// Represents <KvalobsData>...</KvalobsData>
struct KvalobsData {
    #[serde(rename = "station")]
    stations: Vec<Station>,
}
#[derive(Debug, Deserialize)]
/// Represents <station>...</station>
struct Station {
    #[serde(rename = "@val")]
    val: i32,
    #[serde(rename = "typeid")]
    typeids: Vec<Typeid>,
}
#[derive(Debug, Deserialize)]
/// Represents <typeid>...</typeid>
struct Typeid {
    #[serde(rename = "@val")]
    val: i32,
    #[serde(rename = "obstime")]
    obstimes: Vec<Obstime>,
}
#[derive(Debug, Deserialize)]
/// Represents <obstime>...</obstime>
struct Obstime {
    #[serde(rename = "@val")]
    val: String, // avoiding parsing time at this point...
    #[serde(rename = "tbtime")]
    tbtimes: Vec<Tbtime>,
}
#[derive(Debug, Deserialize)]
/// Represents <tbtime>...</tbtime>
struct Tbtime {
    #[serde(rename = "@val")]
    _val: String, // avoiding parsing time at this point...
    _kvtextdata: Option<Vec<Kvtextdata>>,
    #[serde(rename = "sensor")]
    sensors: Vec<Sensor>,
}
/// Represents <kvtextdata>...</kvtextdata>
#[derive(Debug, Deserialize)]
struct Kvtextdata {
    _paramid: Option<i32>,
    _original: Option<String>,
}
#[derive(Debug, Deserialize)]
/// Represents <sensor>...</sensor>
struct Sensor {
    #[serde(rename = "@val", deserialize_with = "zero_to_none")]
    val: Option<i32>,
    #[serde(rename = "level")]
    levels: Vec<Level>,
}
/// Represents <level>...</level>
#[derive(Debug, Deserialize)]
struct Level {
    #[serde(rename = "@val", deserialize_with = "zero_to_none")]
    val: Option<i32>,
    kvdata: Option<Vec<Kvdata>>,
}

// Deserialize sensor and level to null if they are 0
// 0 is the default for kvalobs, but through obsinn it's actually just missing
fn zero_to_none<'de, D>(des: D) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::deserialize(des).map(|opt| match opt {
        Some("0") | Some("") | None => None,
        Some(val) => Some(val.parse::<i32>().unwrap()),
    })
}

/// Represents <kvdata>...</kvdata>
#[derive(Debug, Deserialize)]
pub struct Kvdata {
    #[serde(rename = "@paramid")]
    paramid: i32,
    #[serde(default, deserialize_with = "optional")]
    original: Option<f32>,
    #[serde(default, deserialize_with = "optional")]
    corrected: Option<f32>,
    #[serde(default, deserialize_with = "optional")]
    controlinfo: Option<String>,
    #[serde(default, deserialize_with = "optional")]
    useinfo: Option<String>,
    #[serde(default, deserialize_with = "optional")]
    cfailed: Option<i32>,
}

// If the field is either empty or missing it should deserialize to None.
// The latter is ensured by the #[serde(default)] macro,
// while this function takes care of the former case.
fn optional<'de, D, T>(des: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    Option::deserialize(des).map(|opt| match opt {
        Some("") | None => None,
        Some(val) => Some(val.parse::<T>().unwrap()),
    })
}

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

pub async fn read_and_insert(pool: PgConnectionPool, group_string: String) {
    let (tx, mut rx) = mpsc::channel(10);

    tokio::spawn(async move {
        read_kafka(group_string, tx).await;
    });

    let client = pool.get().await.expect("couldn't connect to database");
    while let Some(msg) = rx.recv().await {
        if let Err(e) = insert_kvdata(&client, msg).await {
            eprintln!("Database insert error: {e}");
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
                            eprintln!("{}", Error::IssueParsingTime(e));
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

async fn read_kafka(group_name: String, tx: mpsc::Sender<Msg>) {
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
        // https://docs.rs/kafka/latest/src/kafka/consumer/mod.rs.html#155
        // poll asks for next available chunk of data as a MessageSet
        match consumer.poll() {
            Ok(sets) => {
                for msgset in sets.iter() {
                    for msg in msgset.messages() {
                        if let Err(e) = parse_message(msg.value, &tx).await {
                            eprintln!("{}", e);
                        }
                    }
                    if let Err(e) = consumer.consume_messageset(msgset) {
                        eprintln!("{}", e);
                    }
                }
                consumer
                    .commit_consumed()
                    .expect("could not commit offset in consumer"); // ensure we keep offset
            }
            Err(e) => {
                eprintln!("{}\nRetrying in 5 seconds...", Error::Kafka(e));
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
    }
}

pub async fn insert_kvdata(
    client: &tokio_postgres::Client,
    Msg {
        kvid,
        obstime,
        kvdata,
    }: Msg,
) -> Result<(), Error> {
    // query timeseries ID
    // NOTE: alternately could use conn.query_one, since we want exactly one response
    let tsid: i32 = client
        .query(
            "SELECT timeseries FROM labels.met 
                WHERE station_id = $1 \
                AND param_id = $2 \
                AND type_id = $3 \
                AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4)) \
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
        .ok_or(Error::TimeseriesMissing(kvid.station, kvid.paramid))?
        .get(0);

    // write the data into the db
    client.execute(
        "INSERT INTO flags.kvdata (timeseries, obstime, original, corrected, controlinfo, useinfo, cfailed)
            VALUES($1, $2, $3, $4, $5, $6, $7)",
        &[&tsid, &obstime, &kvdata.original, &kvdata.corrected, &kvdata.controlinfo, &kvdata.useinfo, &kvdata.cfailed],
    ).await?;

    Ok(())
}
