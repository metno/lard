// https://gitlab.met.no/oda/oda/-/blob/main/internal/services/oda-kvkafka/consume-checked.go?ref_type=heads
// https://gitlab.met.no/tjenester/oda/-/blob/dev/base/oda-kvkafka/deployment.yaml?ref_type=heads

use chrono::NaiveDateTime;
use quick_xml::de::from_str;
use serde::Deserialize;
use substring::Substring;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

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
    // TODO: handle text data
    kvtextdata: Option<Vec<()>>, // just ignoring this for now
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
    cfailed:Option<String>, 
}
#[derive(Debug, Deserialize)]
struct Tsid {
    station: i32,
    paramid: i32,
    typeid: i32,
    sensor: i32,
    level: i32, 
}

fn main() {
    
    let mut consumer =
    Consumer::from_hosts(vec!("kafka2-a1.met.no:9092".to_owned()))
      .with_topic_partitions("kvalobs.production.checked".to_owned(), &[0, 1])
      .with_fallback_offset(FetchOffset::Earliest)
      .with_group("lard-test".to_owned())
      .with_offset_storage(Some(GroupOffsetStorage::Kafka))
      .create()
      .unwrap();

    let mut i = 1;

    loop {
        
        for ms in consumer.poll().unwrap().iter() {

            for m in ms.messages() {
                // do some basic trimming / processing of message
                let xmlmsg = std::str::from_utf8(m.value).unwrap().trim().replace('\n', "").replace("\\", "");

                // do some checking / further processing of message
                if !xmlmsg.starts_with("<?xml") {
                    println!("{:?}", "kv2kvdata must be xml starting with '<?xml '");
                }
                // dangerous unwrap? but expect to find it if have found the first half?
                let kvalobs_xmlmsg = xmlmsg.substring(xmlmsg.find("?>").unwrap()+2, xmlmsg.len());
                //println!("kvalobsdata message: {:?} \n", kvalobs_xmlmsg);

                let item: KvalobsData = from_str(kvalobs_xmlmsg).unwrap();
                //println!("kvalobsdata item: {:?} \n", item);

                // get the useful stuff out of this struct?
                for station in item.station {
                    for typeid in station.typeid {
                        for obstime in typeid.obstime {
                            let obs_time = NaiveDateTime::parse_from_str(&obstime.val, "%Y-%m-%d %H:%M:%S").unwrap();
                            println!("ObsTime: {:?} \n", obs_time);
                            for tbtime in obstime.tbtime {
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
                                                println!("Timeseries Identificator: {:?} \n", tsid);
                                                println!("data: {:?} \n", d);
                                                // TODO: write into db
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
        consumer.commit_consumed().unwrap();

        i -= 1;
        
        if i == 0 {
            // exit loop 
            break;
        }
    }
}
