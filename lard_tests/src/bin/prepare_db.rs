// use std::env;
use std::fs;

use chrono::{DateTime, Duration, DurationRound, TimeDelta, TimeZone, Utc};
use tokio_postgres::{Client, Error, NoTls};

const CONNECT_STRING: &str = "host=localhost user=postgres dbname=postgres password=postgres";

struct Param<'a> {
    id: i32,
    code: &'a str,
}

// TODO: maybe merge into fake_data_generator, a lot of the code is shared
struct Labels<'a> {
    // Assigned automatically
    // timeseries: i32,
    station_id: i32,
    param: Param<'a>,
    type_id: i32,
    level: Option<i32>,
    sensor: Option<i32>,
}

struct Location {
    lat: f32,
    lon: f32,
    // hamsl: f32,
    // hag: f32,
}

struct Timeseries {
    from: DateTime<Utc>,
    period: Duration,
    len: i32,
    deactivated: bool,
    loc: Location,
}

impl Timeseries {
    fn end_time(&self) -> DateTime<Utc> {
        self.from + self.period * self.len
    }
}

struct Case<'a> {
    title: &'a str,
    ts: Timeseries,
    meta: Labels<'a>,
}

async fn create_single_ts(client: &Client, ts: Timeseries) -> Result<i32, Error> {
    let end_time = ts.end_time();

    // Insert timeseries
    let id = match ts.deactivated {
        true => client
            .query_one(
                "INSERT INTO public.timeseries (fromtime, totime, loc.lat, loc.lon, deactivated)
                     VALUES ($1, $2, $3, $4, true) RETURNING id",
                &[&ts.from, &end_time, &ts.loc.lat, &ts.loc.lon],
            )
            .await?
            .get(0),
        false => client
            .query_one(
                "INSERT INTO public.timeseries (fromtime, loc.lat, loc.lon, deactivated) 
                     VALUES ($1, $2, $3, false) RETURNING id",
                &[&ts.from, &ts.loc.lat, &ts.loc.lon],
            )
            .await?
            .get(0),
    };

    // insert data
    let mut value: f32 = 0.0;
    let mut time = ts.from;
    while time <= end_time {
        client
            .execute(
                "INSERT INTO public.data (timeseries, obstime, obsvalue) 
                    VALUES ($1, $2, $3)",
                &[&id, &time, &value],
            )
            .await?;
        time += ts.period;
        value += 1.0;
    }

    Ok(id)
}

async fn insert_ts_metadata<'a>(client: &Client, id: i32, meta: Labels<'a>) -> Result<(), Error> {
    client
        .execute(
            "INSERT INTO labels.met (timeseries, station_id, param_id, type_id, lvl, sensor) 
                 VALUES($1, $2, $3, $4, $5, $6)",
            &[
                &id,
                &meta.station_id,
                &meta.param.id,
                &meta.type_id,
                &meta.level,
                &meta.sensor,
            ],
        )
        .await?;

    client
        .execute(
            "INSERT INTO labels.obsinn (timeseries, nationalnummer, type_id, param_code, lvl, sensor) 
                 VALUES($1, $2, $3, $4, $5, $6)",
            &[
                &id,
                &meta.station_id,
                &meta.type_id,
                &meta.param.code,
                &meta.level,
                &meta.sensor,
            ],
        )
        .await?;

    Ok(())
}

async fn create_timeseries(client: &Client) -> Result<(), Error> {
    let cases = vec![
        Case {
            title: "Daily, active",
            ts: Timeseries {
                from: Utc.with_ymd_and_hms(1970, 6, 5, 0, 0, 0).unwrap(),
                period: Duration::days(1),
                len: 19,
                loc: Location {
                    lat: 59.9,
                    lon: 10.4,
                },
                deactivated: false,
            },
            meta: Labels {
                station_id: 10000,
                param: Param {
                    id: 103,
                    code: "EV_24", // sum(water_evaporation_amount)
                },
                type_id: 1, // Is there a type_id for daily data?
                level: Some(0),
                sensor: Some(0),
            },
        },
        Case {
            title: "Hourly, active",
            ts: Timeseries {
                from: Utc.with_ymd_and_hms(2012, 2, 14, 0, 0, 0).unwrap(),
                period: Duration::hours(1),
                len: 47,
                loc: Location {
                    lat: 46.0,
                    lon: -73.0,
                },
                deactivated: false,
            },
            meta: Labels {
                station_id: 11000,
                param: Param {
                    id: 222,
                    code: "TGM", // mean(grass_temperature)
                },
                type_id: 501, // hourly data
                level: Some(0),
                sensor: Some(0),
            },
        },
        Case {
            title: "Minutely, active 1",
            ts: Timeseries {
                from: Utc.with_ymd_and_hms(2023, 5, 5, 0, 0, 0).unwrap(),
                period: Duration::minutes(1),
                len: 99,
                loc: Location {
                    lat: 65.89,
                    lon: 13.61,
                },
                deactivated: false,
            },
            meta: Labels {
                station_id: 12000,
                param: Param {
                    id: 211,
                    code: "TA", // air_temperature
                },
                type_id: 508, // minute data
                level: None,
                sensor: None,
            },
        },
        Case {
            title: "Minutely, active 2",
            ts: Timeseries {
                from: Utc.with_ymd_and_hms(2023, 5, 5, 0, 0, 0).unwrap(),
                period: Duration::minutes(1),
                len: 99,
                loc: Location {
                    lat: 66.0,
                    lon: 14.0,
                },
                deactivated: false,
            },
            meta: Labels {
                station_id: 12100,
                param: Param {
                    id: 255,
                    code: "TWD", // sea_water_temperature
                },
                type_id: 508, // minute data
                level: Some(0),
                sensor: Some(0),
            },
        },
        Case {
            // use it to test latest endpoint without optional query
            title: "3hrs old minute data",
            ts: Timeseries {
                from: Utc::now().duration_trunc(TimeDelta::minutes(1)).unwrap()
                    - Duration::minutes(179),
                period: Duration::minutes(1),
                len: 179,
                loc: Location { lat: 1.0, lon: 1.0 },
                deactivated: false,
            },
            meta: Labels {
                station_id: 20000,
                param: Param {
                    id: 211,
                    code: "TA", // air_temperature
                },
                type_id: 508, // minute data
                level: Some(0),
                sensor: Some(0),
            },
        },
        Case {
            // use it to test stations endpoint with optional time resolution (PT1H)
            title: "Air temperature over the last 12 hours",
            ts: Timeseries {
                // TODO: check that this adds the correct number of data points every time
                from: Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap() - Duration::hours(11),
                period: Duration::hours(1),
                len: 11,
                loc: Location { lat: 2.0, lon: 2.0 },
                deactivated: false,
            },
            meta: Labels {
                station_id: 30000,
                param: Param {
                    id: 211,
                    code: "TA", // air_temperature
                },
                type_id: 501, // hourly data
                level: Some(0),
                sensor: Some(0),
            },
        },
    ];

    for case in cases {
        println!("Inserting timeseries: {}", case.title);
        let id = create_single_ts(client, case.ts).await?;
        insert_ts_metadata(client, id, case.meta).await?;
    }

    Ok(())
}

async fn cleanup(client: &Client) -> Result<(), Error> {
    client
        .batch_execute("DROP TABLE IF EXISTS timeseries, data, labels.met, labels.obsinn CASCADE")
        .await?;
    client.batch_execute("DROP TYPE IF EXISTS location").await?;

    Ok(())
}

async fn create_schema(client: &Client) -> Result<(), Error> {
    let public_schema =
        fs::read_to_string("db/public.sql").expect("Should be able to read SQL file");
    client.batch_execute(public_schema.as_str()).await?;

    let labels_schema =
        fs::read_to_string("db/labels.sql").expect("Should be able to read SQL file");
    client.batch_execute(labels_schema.as_str()).await?;

    Ok(())
}

async fn create_partitions(client: &Client) -> Result<(), Error> {
    // TODO: add multiple partitions?
    let partition_string = format!(
        "CREATE TABLE data_y{}_to_y{} PARTITION OF public.data FOR VALUES FROM ('{}') TO ('{}')",
        "1950", "2100", "1950-01-01 00:00:00+00", "2100-01-01 00:00:00+00",
    );
    client.batch_execute(partition_string.as_str()).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // let _test_type: String = env::args()
    //     .next()
    //     .expect("Provide test type for database setup ('api', 'ingestion', 'e2e')");

    let (client, connection) = tokio_postgres::connect(CONNECT_STRING, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    cleanup(&client).await?;
    create_schema(&client).await?;
    create_partitions(&client).await?;
    create_timeseries(&client).await?;

    Ok(())
}
