// https://gitlab.met.no/oda/oda/-/blob/main/internal/services/oda-kvkafka/consume-checked.go?ref_type=heads
// https://gitlab.met.no/tjenester/oda/-/blob/dev/base/oda-kvkafka/deployment.yaml?ref_type=heads

use chrono::NaiveDateTime;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use quick_xml::de::from_str;
use serde::Deserialize;
use std::env;
use substring::Substring;
use thiserror::Error;
use tokio_postgres::NoTls;

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error(
        "no Timeseries found for this data - station {:?}, param {:?}",
        station,
        param
    )]
    NoTs { station: i32, param: i32 },
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
    kvtextdata: Option<Vec<Kvtextdata>>,
}
#[derive(Debug, Deserialize)]
/// Represents <sensor>...</sensor>
struct Sensor {
    #[serde(rename = "@val")]
    val: i32,
    level: Vec<Level>,
}
/// Represents <level>...</level>
#[derive(Debug, Deserialize)]
struct Level {
    #[serde(rename = "@val")]
    val: i32,
    kvdata: Option<Vec<Kvdata>>,
}
/// Represents <kvdata>...</kvdata>
#[derive(Debug, Deserialize)]
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
#[derive(Debug, Deserialize)]
struct Tsid {
    station: i32,
    paramid: i32,
    typeid: i32,
    sensor: i32,
    level: i32,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();

    assert!(args.len() >= 4, "not enough args passed in, at least host, user, dbname needed, optionally password");

    let mut connect_string = format!("host={} user={} dbname={}", &args[1], &args[2], &args[3]);
    if args.len() > 4 {
        connect_string.push_str(" password=");
        connect_string.push_str(&args[4]);
    }

    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(connect_string.as_str(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // NOTE: reading from the 4 redundant kafka queues, but only reading the checked data (other topics exists)
    let mut consumer = Consumer::from_hosts(vec![
        "kafka2-a1.met.no:9092".to_owned(),
        "kafka2-a2.met.no:9092".to_owned(),
        "kafka2-b1.met.no:9092".to_owned(),
        "kafka2-b2.met.no:9092".to_owned(),
    ])
    .with_topic_partitions("kvalobs.production.checked".to_owned(), &[0, 1])
    .with_fallback_offset(FetchOffset::Earliest)
    .with_group("lard-test".to_owned())
    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
    .create()
    .unwrap();

    // Consume the kafka queue infinitely 
    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                // do some basic trimming / processing of message
                let xmlmsg = std::str::from_utf8(m.value)
                    .unwrap()
                    .trim()
                    .replace(['\n', '\\'], "");

                // do some checking / further processing of message
                if !xmlmsg.starts_with("<?xml") {
                    println!("{:?}", "kv2kvdata must be xml starting with '<?xml '");
                }
                let kvalobs_xmlmsg = xmlmsg.substring(xmlmsg.find("?>").unwrap() + 2, xmlmsg.len());
                //println!("kvalobsdata message: {:?} \n", kvalobs_xmlmsg);

                let item: KvalobsData = from_str(kvalobs_xmlmsg).unwrap();
                //println!("kvalobsdata item: {:?} \n", item);

                // get the useful stuff out of this struct
                for station in item.station {
                    for typeid in station.typeid {
                        for obstime in typeid.obstime {
                            let obs_time =
                                NaiveDateTime::parse_from_str(&obstime.val, "%Y-%m-%d %H:%M:%S")
                                    .unwrap();
                            println!("ObsTime: {obs_time:?} \n");
                            for tbtime in obstime.tbtime {
                                let tb_time = NaiveDateTime::parse_from_str(
                                    &tbtime.val,
                                    "%Y-%m-%d %H:%M:%S%.6f",
                                )
                                .unwrap();
                                // NOTE: this is "table time" which can vary from the actual observation time,
                                // its the first time in entered the db in kvalobs
                                println!("TbTime: {tb_time:?} \n");
                                if let Some(textdata) = tbtime.kvtextdata {
                                    // TODO: Do we want to handle text data at all, it doesn't seem to be QCed
                                    println!(
                                        "station, typeid, textdata: {:?} {:?} {:?} \n",
                                        station.val, typeid.val, textdata
                                    );
                                }
                                for sensor in tbtime.sensor {
                                    for level in sensor.level {
                                        if let Some(data) = level.kvdata {
                                            for d in data {
                                                let tsid = Tsid {
                                                    station: station.val,
                                                    paramid: d.paramid,
                                                    typeid: typeid.val,
                                                    sensor: sensor.val,
                                                    level: level.val,
                                                };
                                                println!("Timeseries ID: {tsid:?} \n");
                                                println!("Data: {d:?} \n");
                                                // Try to write into db
                                                let _ = insert_kvdata(&client, tsid, obs_time, d)
                                                    .await
                                                    .map_err(|e| {
                                                        eprintln!(
                                                            "Writing to database error: {e:?}"
                                                        );
                                                    });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            let result = consumer.consume_messageset(ms);
            if result.is_err() {
                println!("{:?}", result.err());
            }
        }
        consumer.commit_consumed().unwrap(); // ensure we keep offset
    }
}

async fn insert_kvdata(
    conn: &tokio_postgres::Client,
    tsid: Tsid,
    obstime: chrono::NaiveDateTime,
    kvdata: Kvdata,
) -> Result<(), Error> {
    // what timeseries is this?
    let tsid: i64 = conn.query(
        "SELECT timeseries FROM labels.met WHERE station_id = $1 AND param_id = $2 AND type_id = $3 AND lvl = $4 AND sensor = $5",
        &[&tsid.station, &tsid.paramid, &tsid.typeid, &tsid.level, &tsid.sensor],
    ).await?.first().ok_or( Error::NoTs{station: tsid.station, param: tsid.paramid})?.get(0);

    // write the data into the db
    conn.execute(
        "INSERT INTO flags.kvdata (timeseries, obstime, original, corrected, controlinfo, useinfo, cfailed) VALUES($1, $2, $3, $4, $5, $6, $7)",
        &[&tsid, &obstime, &kvdata.original, &kvdata.corrected, &kvdata.controlinfo, &kvdata.useinfo, &kvdata.cfailed],
    ).await?;

    Ok(())
}
