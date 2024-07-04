use chrono::{DateTime, NaiveDateTime, Utc};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use quick_xml::de::from_str;
use serde::{Deserialize, Deserializer};
use std::env;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_postgres::{types::ToSql, NoTls};

#[derive(Error, Debug)]
pub enum Error {
    #[error("parsing xml error: {0}")]
    IssueParsingXML(String),
    #[error("parsing time error: {0}")]
    IssueParsingTime(#[from] chrono::ParseError),
    #[error("postgres returned an error: {0}")]
    Kafka(#[from] kafka::Error),
    #[error("kafka returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error(
        "no Timeseries ID found for this data - station {}, param {}",
        station,
        param
    )]
    TimeseriesMissing { station: i32, param: i32 },
}

#[derive(Debug, Deserialize)]
/// Represents <KvalobsData>...</KvalobsData>
struct KvalobsData {
    station: Vec<Stations>,
}
#[derive(Debug, Deserialize)]
/// Represents <station>...</station>
struct Stations {
    #[serde(rename = "@val")]
    val: i32,
    typeid: Vec<Typeid>,
}
#[derive(Debug, Deserialize)]
/// Represents <typeid>...</typeid>
struct Typeid {
    #[serde(rename = "@val")]
    val: i32,
    obstime: Vec<Obstime>,
}
#[derive(Debug, Deserialize)]
/// Represents <obstime>...</obstime>
struct Obstime {
    #[serde(rename = "@val")]
    val: String, // avoiding parsing time at this point...
    tbtime: Vec<Tbtime>,
}
#[derive(Debug, Deserialize)]
/// Represents <tbtime>...</tbtime>
struct Tbtime {
    #[serde(rename = "@val")]
    _val: String, // avoiding parsing time at this point...
    _kvtextdata: Option<Vec<Kvtextdata>>,
    sensor: Vec<Sensor>,
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
    level: Vec<Level>,
}
/// Represents <level>...</level>
#[derive(Debug, Deserialize)]
struct Level {
    #[serde(rename = "@val", deserialize_with = "zero_to_none")]
    val: Option<i32>,
    kvdata: Option<Vec<Kvdata>>,
}

// Change the sensor and level back to null if they are 0
// 0 is the default for kvalobs, but through obsinn its actually just missing
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
#[derive(Debug, Deserialize, ToSql)]
struct Kvdata {
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

fn optional<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    Option::deserialize(deserializer).map(|opt| match opt {
        Some("") | None => None,
        Some(val) => Some(val.parse::<T>().unwrap()),
    })
}

#[derive(Debug, Deserialize, ToSql)]
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

#[tokio::main]
async fn main() {
    // parse args
    let args: Vec<String> = env::args().collect();

    assert!(args.len() >= 5, "Not enough args passed in, at least group name (for kafka), host, user, dbname needed, and optionally password (for the database)");

    let mut connect_string = format!("host={} user={} dbname={}", &args[2], &args[3], &args[4]);
    if args.len() > 5 {
        connect_string.push_str(" password=");
        connect_string.push_str(&args[5]);
    }
    println!(
        "Connecting to database with string: {:?} \n",
        connect_string
    );

    let group_string = format!("{}", &args[1]);
    println!(
        "Creating kafka consumer with group name: {:?} \n",
        group_string
    );

    // create a channel
    let (tx, mut rx) = mpsc::channel(10);

    // start read kafka task
    tokio::spawn(async move {
        read_kafka(group_string, tx).await;
    });

    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(connect_string.as_str(), NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });

    // Start receiving messages
    while let Some(msg) = rx.recv().await {
        // write to db task
        if let Err(insert_err) = insert_kvdata(&client, msg.kvid, msg.obstime, msg.kvdata).await {
            eprintln!("Database insert error: {insert_err}");
        }
    }
}

async fn parse_message(message: &[u8], tx: &mpsc::Sender<Msg>) -> Result<(), Error> {
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

    let kvalobs_xmlmsg = match xmlmsg.find("?>") {
        Some(loc) => &xmlmsg[(loc + 2)..],
        None => {
            return Err(Error::IssueParsingXML(
                "couldn't find end of xml tag '?>'".to_string(),
            ))
        }
    };
    let item: KvalobsData = from_str(kvalobs_xmlmsg).unwrap();

    // get the useful stuff out of this struct
    for station in item.station {
        for typeid in station.typeid {
            for obstime in typeid.obstime {
                let obs_time =
                    NaiveDateTime::parse_from_str(&obstime.val, "%Y-%m-%d %H:%M:%S")?.and_utc();
                for tbtime in obstime.tbtime {
                    // NOTE: this is "table time" which can vary from the actual observation time,
                    // its the first time it entered the db in kvalobs
                    //
                    // currently not using this, so can just log and keep going?
                    // let tb_time =
                    //      NaiveDateTime::parse_from_str(&tbtime._val, "%Y-%m-%d %H:%M:%S%.6f");
                    // if tb_time.is_err() {
                    //     eprintln!(
                    //         "{}",
                    //         Error::IssueParsingTime(tb_time.expect_err("cannot parse tbtime"))
                    //     );
                    // }
                    //
                    // TODO: Do we want to handle text data at all, it doesn't seem to be QCed
                    // if let Some(textdata) = tbtime.kvtextdata {
                    //     println!(
                    //         "station {:?}, typeid {:?}, textdata: {:?}",
                    //         station.val, typeid.val, textdata
                    //     );
                    // }
                    //
                    for sensor in tbtime.sensor {
                        for level in sensor.level {
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
        let poll_result = consumer.poll();
        if poll_result.is_err() {
            eprintln!("{}", Error::Kafka(poll_result.unwrap_err()));
        } else {
            for msgset in poll_result.expect("issue calling poll ").iter() {
                for msg in msgset.messages() {
                    if let Err(e) = parse_message(msg.value, &tx).await {
                        println!("{}", e);
                    }
                }
                if let Err(e) = consumer.consume_messageset(msgset) {
                    println!("{}", e);
                }
            }
            consumer
                .commit_consumed()
                .expect("could not commit offset in consumer"); // ensure we keep offset
        }
    }
}

async fn insert_kvdata(
    client: &tokio_postgres::Client,
    kvid: KvalobsId,
    obstime: DateTime<Utc>,
    kvdata: Kvdata,
) -> Result<(), Error> {
    // what timeseries is this?
    // NOTE: alternately could use conn.query_one, since we want exactly one response
    let tsid: i64 = client
        .query(
            "SELECT timeseries FROM labels.met WHERE station_id = $1 \
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
        .ok_or(Error::TimeseriesMissing {
            station: kvid.station,
            param: kvid.paramid,
        })?
        .get(0);

    // write the data into the db
    // kvdata derives ToSql therefore options should be nullable
    // https://docs.rs/postgres-types/latest/postgres_types/trait.ToSql.html#nullability
    client.execute(
        "INSERT INTO flags.kvdata (timeseries, obstime, original, corrected, controlinfo, useinfo, cfailed) VALUES($1, $2, $3, $4, $5, $6, $7)",
        &[&tsid, &obstime, &kvdata.original, &kvdata.corrected, &kvdata.controlinfo, &kvdata.useinfo, &kvdata.cfailed],
    ).await?;

    Ok(())
}
