use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use futures::pin_mut;
use rand::{seq::SliceRandom, Rng};
use std::{env, fs, time::Instant};
use tokio_postgres::{
    types::{FromSql, ToSql, Type},
    NoTls,
};

#[derive(Debug, ToSql, FromSql)]
struct Data {
    timeseries: i32,
    obstime: DateTime<Utc>,
    obsvalue: f32,
}

async fn cleanup_setup(client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    // cleanup stuff before?
    client
        .execute(
            "DROP TABLE IF EXISTS timeseries, data, labels.filter CASCADE",
            &[],
        )
        .await?;
    client
        .execute("DROP TYPE IF EXISTS location, filterlabel", &[])
        .await?;

    // insert the schema(s)
    let contents_public = fs::read_to_string("db/public.sql")
        .expect("Should have been able to read the public schema file");

    //println!("Public:\n{contents_public}");
    client.batch_execute(contents_public.as_str()).await?;
    println!("Finished inserting public schema");

    let contents_labels = fs::read_to_string("db/labels.sql")
        .expect("Should have been able to read the public schema file");

    //println!("Labels:\n{contents_labels}");
    client.batch_execute(contents_labels.as_str()).await?;
    println!("Finished inserting labels schema");

    Ok(())
}

fn random_element() -> &'static str {
    // list of elements
    const ELEMENTS: &[&str] = &["air_temperature", "precipitation", "wind_speed"];
    let mut rng = rand::thread_rng();
    let el = ELEMENTS.choose(&mut rng);
    match el {
        Some(x) => x,
        None => "unknown",
    }
}

async fn create_timeseries(
    client: &tokio_postgres::Client,
    num_ts: i32,
) -> Result<Vec<(i32, DateTime<Utc>)>, tokio_postgres::Error> {
    // create rand
    let mut rng = rand::thread_rng();
    // keep list of ts with date
    let mut timeseries = Vec::new();

    // insert a bunch of timeseries
    for x in 1..num_ts + 1 {
        let weeks = rng.gen_range(12..4000); // 1 year to 70 something years back (but in weeks)
        let now = Utc::now();
        let random_past_date = Utc
            .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
            .unwrap()
            - Duration::weeks(weeks);
        let random_lat = rng.gen_range(59..72) as f32 * 0.5;
        let random_lon = rng.gen_range(4..30) as f32 * 0.5;
        let lat = random_lat;
        let lon = random_lon;
        println!(
            "Insert timeseries {} {} {} {} {}",
            random_past_date, random_lat, random_lon, lat, lon
        );
        client
                .execute(
                    "INSERT INTO timeseries (fromtime, loc.lat, loc.lon, deactivated) VALUES($1, $2, $3, false)",
                    &[&random_past_date, &lat, &lon],
                )
                .await?;
        timeseries.push((x, random_past_date));

        // also label the timeseries
        let random_station_id = rng.gen_range(1000..2000) as f32;
        let random_element_id = random_element();
        let level = 0;
        let sensor = 0;
        client.execute(
                "INSERT INTO labels.filter (timeseries, label.stationID, label.elementID, label.lvl, label.sensor) VALUES($1, $2, $3, $4, $5)",
                &[&x.to_owned(), &random_station_id.to_owned(), &random_element_id, &level.to_owned(), &sensor.to_owned()],
            ).await?;
    }
    Ok(timeseries)
}

fn create_data_vec(timeseries: &Vec<(i32, DateTime<Utc>)>) -> Vec<[Box<dyn ToSql + Sync>; 3]> {
    let mut data_vec: Vec<[Box<dyn ToSql + Sync>; 3]> = Vec::new();
    let mut rng = rand::thread_rng();
    for (id, t) in timeseries {
        // insert hourly data from the past data until now
        let mut time = *t;
        let now = Utc::now();
        let round_now: DateTime<Utc> = Utc
            .with_ymd_and_hms(now.year(), now.month(), now.day(), now.hour(), 0, 0)
            .unwrap();
        while time <= round_now {
            let v = rng.gen_range(0..30) as f32 * 0.5;
            data_vec.push([Box::new(*id), Box::new(time), Box::new(v)]);

            time += Duration::hours(1);
        }
    }

    data_vec
}

async fn copy_in_data(
    client: &mut tokio_postgres::Client,
    data_vec: Vec<[Box<dyn ToSql + Sync>; 3]>,
) -> Result<(), tokio_postgres::Error> {
    let start_copy_in = Instant::now();
    let tx = client.transaction().await?;
    let sink = tx.copy_in("COPY data FROM STDIN BINARY").await?;
    let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(
        sink,
        &[Type::INT4, Type::TIMESTAMPTZ, Type::FLOAT4],
    );
    pin_mut!(writer);
    for row in data_vec.iter() {
        writer
            .as_mut()
            .write_raw(row.iter().map(|s| s.as_ref()))
            .await?;
    }
    writer.finish().await?;
    tx.commit().await?;
    println!(
        "Time elapsed copying {} fake data is: {:?}",
        data_vec.len(),
        start_copy_in.elapsed()
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), tokio_postgres::Error> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        panic!("not enough args passed in, at least host, user, dbname needed, optionally password")
    }

    let mut connect_string = format!("host={} user={} dbname={}", &args[1], &args[2], &args[3]);
    if args.len() > 4 {
        connect_string.push(' ');
        connect_string.push_str(&args[4])
    }

    // Connect to the database.
    let (mut client, connection) = tokio_postgres::connect(connect_string.as_str(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // clear things, and setup again
    cleanup_setup(&client).await?;

    // create random timeseries
    let ts_map = create_timeseries(&client, 10).await?;

    // create data
    println!("Making fake data...");
    let data_vec = create_data_vec(&ts_map);

    println!("Copy in data...");
    copy_in_data(&mut client, data_vec).await?;

    Ok(())
}
