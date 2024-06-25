use chrono::NaiveDateTime;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use quick_xml::de::from_str;
use serde::Deserialize;
use std::env;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_postgres::{types::ToSql, NoTls};

#[derive(Error, Debug)]
pub enum Error {
    #[error("parsing xml error: {:?}, XML: {:?}", error, xml)]
    IssueParsingXML { error: String, xml: String },
    #[error("parsing time error: {0}")]
    IssueParsingTime(#[from] chrono::ParseError),
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error(
        "no Timeseries ID found for this data - station {:?}, param {:?}",
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
    val: String, // avoiding parsing time at this point...
    sensor: Vec<Sensor>,
    _kvtextdata: Option<Vec<Kvtextdata>>,
}
#[derive(Debug, Deserialize)]
/// Represents <sensor>...</sensor>
struct Sensor {
    #[serde(rename = "@val")]
    val: Option<i32>,
    level: Vec<Level>,
}
/// Represents <level>...</level>
#[derive(Debug, Deserialize)]
struct Level {
    #[serde(rename = "@val")]
    val: Option<i32>,
    kvdata: Option<Vec<Kvdata>>,
}
/// Represents <kvdata>...</kvdata>
#[derive(Debug, Deserialize, ToSql)]
struct Kvdata {
    #[serde(rename = "@paramid")]
    paramid: i32,
    original: Option<f64>,
    corrected: Option<f64>,
    controlinfo: Option<String>,
    useinfo: Option<String>,
    cfailed: Option<String>,
}
/// Represents <kvtextdata>...</kvtextdata>
#[derive(Debug, Deserialize)]
struct Kvtextdata {
    _paramid: Option<i32>,
    _original: Option<String>,
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
struct Msg {
        kvid: KvalobsId,
        obstime: chrono::NaiveDateTime,
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

async fn read_kafka(group_name: String, tx: tokio::sync::mpsc::Sender<Msg>) {
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
        // no errors are actually returned 
        for ms in consumer.poll().unwrap().iter() {
            'message: for m in ms.messages() {
                // do some basic trimming / processing of the raw message
                // received from the kafka queue
                let raw_xmlmsg = std::str::from_utf8(m.value);
                if !raw_xmlmsg.is_ok() {
                    eprintln!(
                        "{}",
                        Error::IssueParsingXML {
                            error: "couldn't convert message from utf8".to_string(),
                            xml: "".to_string()
                        }
                    );
                    break 'message;
                }
                let xmlmsg = raw_xmlmsg.unwrap().trim().replace(['\n', '\\'], "");

                // do some checking / further processing of message
                if !xmlmsg.starts_with("<?xml") {
                    eprintln!(
                        "{:?}",
                        Error::IssueParsingXML {
                            error: "kv2kvdata must be xml starting with '<?xml'".to_string(),
                            xml: xmlmsg
                        }
                    );
                    break 'message;
                }
                let loc_end_xml_tag = xmlmsg.find("?>");
                // strip the xml string to just the part inside the kvalobsdata tags,
                // by removing the xml tag section at the beginning
                if loc_end_xml_tag.is_none() {
                    eprintln!(
                        "{}",
                        Error::IssueParsingXML {
                            error: "couldn't find end of xml tag '?>'".to_string(),
                            xml: xmlmsg
                        }
                    );
                    break 'message;
                }
                let kvalobs_xmlmsg = &xmlmsg[(loc_end_xml_tag.unwrap() + 2)..(xmlmsg.len())];

                let item: KvalobsData = from_str(kvalobs_xmlmsg).unwrap();

                // get the useful stuff out of this struct
                for station in item.station {
                    for typeid in station.typeid {
                        'obstime: for obstime in typeid.obstime {
                            match NaiveDateTime::parse_from_str(&obstime.val, "%Y-%m-%d %H:%M:%S") {
                                Ok(obs_time) => {
                                    for tbtime in obstime.tbtime {
                                        // NOTE: this is "table time" which can vary from the actual observation time,
                                        // its the first time in entered the db in kvalobs
                                        let tb_time = NaiveDateTime::parse_from_str(
                                            &tbtime.val,
                                            "%Y-%m-%d %H:%M:%S%.6f",
                                        );
                                        if tb_time.is_err() {
                                            eprintln!(
                                                "{}",
                                                Error::IssueParsingTime(
                                                    tb_time.expect_err("cannot parse tbtime")
                                                )
                                            );
                                            // currently not using this, so can just log and keep going?
                                        }
                                        /*
                                        // TODO: Do we want to handle text data at all, it doesn't seem to be QCed
                                        if let Some(textdata) = tbtime.kvtextdata {
                                            println!(
                                                "station {:?}, typeid {:?}, textdata: {:?}",
                                                station.val, typeid.val, textdata
                                            );
                                        }
                                        */
                                        for sensor in tbtime.sensor {
                                            for level in sensor.level {
                                                if let Some(data) = level.kvdata {
                                                    for d in data {
                                                        let kvid = KvalobsId {
                                                            station: station.val,
                                                            paramid: d.paramid,
                                                            typeid: typeid.val,
                                                            sensor: sensor.val,
                                                            level: level.val,
                                                        };
                                                        // Try to write into db
                                                        let cmd = Msg {
                                                            kvid: kvid,
                                                            obstime: obs_time,
                                                            kvdata: d,
                                                        };
                                                        tx.send(cmd).await.unwrap();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("{}", Error::IssueParsingTime(e));
                                    break 'obstime;
                                }
                            }
                        }
                    }
                }
            }
            if let Err(e) = consumer.consume_messageset(ms) {
                println!("{}", e);
            }
        }
        consumer.commit_consumed().expect("could not  offset in consumer"); // ensure we keep offset
    }
}

async fn insert_kvdata(
    client: &tokio_postgres::Client,
    kvid: KvalobsId,
    obstime: chrono::NaiveDateTime,
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
