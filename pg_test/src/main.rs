use chrono::{DateTime, Datelike, Duration, NaiveDateTime, TimeZone, Timelike, Utc};
use futures_util::future;
use postgres_types::{FromSql, ToSql};
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::HashMap;
use std::time::Instant;
use std::{env, fs};
use tokio_postgres::{Error, NoTls};

#[derive(Debug, ToSql, FromSql)]
struct Location {
    _lat: f32,
    _lon: f32,
    _hamsl: f32,
    _hag: f32,
}

#[derive(Debug, ToSql, FromSql)]
struct FilterLabel {
    _stn: f32,
    _elem: String,
    _lvl: i32,
    _sensor: i32,
}

#[derive(Debug, ToSql, FromSql)]
struct Data {
    timeseries: i32,
    timestamp: DateTime<Utc>,
    value: f32,
}

fn random_element() -> String {
    // list of elements
    let elements = vec!["air_temperature", "precipitation", "wind_speed"];
    let mut rng = rand::thread_rng();
    let el = elements.choose(&mut rng);
    match el {
        Some(x) => x.to_string(),
        None => "unknown".to_string(),
    }
}

async fn cleanup_setup(connect_string: &String) -> Result<(), Error> {
    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(connect_string.as_str(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

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
    let contents_public = fs::read_to_string("../db/public.sql")
        .expect("Should have been able to read the public schema file");

    //println!("Public:\n{contents_public}");
    client.batch_execute(contents_public.as_str()).await?;
    println!("Finished inserting public schema");

    let contents_labels = fs::read_to_string("../db/labels.sql")
        .expect("Should have been able to read the public schema file");

    //println!("Labels:\n{contents_labels}");
    client.batch_execute(contents_labels.as_str()).await?;
    println!("Finished inserting labels schema");

    // partman
    client
        .query(
            "DELETE from partman.part_config WHERE parent_table='public.data'",
            &[],
        )
        .await?;
    // create all partions back to 1950
    client
        .execute(
            "SELECT partman.create_parent('public.data', 'timestamp', 'partman', 'monthly', p_start_partition := '1950-01-01')",
            &[],
        )
        .await?;
    let partman = client
        .query(
            "SELECT * from partman.part_config WHERE parent_table='public.data'",
            &[],
        )
        .await?;
    for p in partman {
        println!("partman {:?}", p);
    }
    Ok(())
}

async fn create_timeseries(
    connect_string: &String,
    num_ts: i32,
) -> Result<HashMap<i32, DateTime<Utc>>, Error> {
    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(connect_string.as_str(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // create rand
    let mut rng = rand::thread_rng();
    // keep list of ts with date
    let mut timeseries = HashMap::new();

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
        let loc = Location {
            _lat: random_lat,
            _lon: random_lon,
            _hamsl: 0.0,
            _hag: 0.0,
        };
        println!(
            "Insert timeseries {} {} {} {:?}",
            random_past_date, random_lat, random_lon, loc
        );
        client
                .execute(
                    "INSERT INTO timeseries (fromtime, loc.lat, loc.lon, deactivated) VALUES($1, $2, $3, false)",
                    &[&random_past_date, &loc._lat, &loc._lon],
                )
                .await?;
        timeseries.insert(x, random_past_date);

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

async fn create_data(
    connect_string: &String,
    timeseries: &HashMap<i32, DateTime<Utc>>,
) -> Result<(), Error> {
    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(connect_string.as_str(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // insert data into these timeseries
    println!("Insert data...");
    // create rand
    let mut rng = rand::thread_rng();
    let start_insert = Instant::now();
    for (id, t) in timeseries {
        // insert hourly data from the past data until now
        let mut time = *t;
        let now = Utc::now();
        let round_now: DateTime<Utc> = Utc
            .with_ymd_and_hms(now.year(), now.month(), now.day(), now.hour(), 0, 0)
            .unwrap();
        let mut tss: Vec<i32> = Vec::new();
        let mut times: Vec<NaiveDateTime> = Vec::new();
        let mut values: Vec<f32> = Vec::new();
        while time <= round_now {
            let v = rng.gen_range(0..30) as f32 * 0.5;
            tss.push(*id);
            times.push(time.naive_utc());
            values.push(v);
            // increment
            time += Duration::hours(1);
        }
        println!("Inserting {} pieces of data", tss.len());
        // documentation indicates unnest is a good way to do batch inserts in postgres
        client
                    .query(
                        "INSERT INTO data (timeseries, timestamp, value) SELECT * FROM UNNEST($1::INT[], $2::TIMESTAMP[], $3::REAL[])",
                        &[&tss, &times, &values],
                    )
                    .await?;
    }
    println!(
        "Time elapsed inserting fake data is: {:?}",
        start_insert.elapsed()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    print!("{:?}", args);

    if args.len() != 5 {
        eprintln!("not all parameters passed in on command line, will crash when connecting!")
    }

    let host = &args[1];
    let user = &args[2];
    let dbname = &args[3];
    let password = &args[4];

    let mut connect_string: String = "host=".to_owned();
    connect_string.push_str(&host);
    connect_string.push_str(" user=");
    connect_string.push_str(&user);
    connect_string.push_str(" dbname=");
    connect_string.push_str(&dbname);
    connect_string.push_str(" password=");
    connect_string.push_str(&password);
    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(connect_string.as_str(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // clear things, and setup again
    cleanup_setup(&connect_string).await?;

    // create random timeseries
    let timeseries = create_timeseries(&connect_string, 1000).await?;

    // create data
    create_data(&connect_string, &timeseries).await?;

    // get timeseries out of the database, with labels
    println!("List timeseries with air_temperature...");
    for row in client
        .query(
            "SELECT timeseries.id, (timeseries.loc).lat, (timeseries.loc).lon, (filter.label).stationID, (filter.label).elementID FROM public.timeseries 
            JOIN labels.filter
            ON timeseries.id = labels.filter.timeseries
            WHERE (filter.label).elementID='air_temperature'",
            &[],
        )
        .await?
    {
        let id: i32 = row.get(0);
        let loc = Location {
            _lat: row.get(1),
            _lon: row.get(2),
            _hamsl: 0.0,
            _hag: 0.0,
        };
        let label = FilterLabel {
            _stn: row.get(3),
            _elem: row.get(4),
            _lvl: 0,
            _sensor: 0,
        };

        println!("{} {:?} {:?}", id, loc, label);
    }

    println!("Retrieve data...");
    // get just the latest data back out
    let start_select_latest = Instant::now();
    let latest = client
        .query(
            "SELECT timeseries, timestamp, value FROM data WHERE data.timestamp > (NOW() at time zone 'utc' - INTERVAL '1 HOUR');",
            &[],
        )
        .await?;
    println!(
        "Time elapsed getting {} latest fake data is: {:?}",
        latest.len(),
        start_select_latest.elapsed()
    );

    // actually get all the data back out
    let start_select = Instant::now();
    // construct sql for retrieving all the data
    let mut retrieve_data_futures = vec![];
    for (id, t) in &timeseries {
        // scope?
        let client = &client;
        let from_time = (*t).format("%Y-%m-%d").to_string();
        let mut select_string: String =
            "SELECT timeseries, timestamp, value FROM data WHERE timeseries='".to_owned();
        select_string.push_str(&id.to_string());
        select_string.push_str("' AND data.timestamp BETWEEN '");
        select_string.push_str(from_time.as_str());
        select_string.push_str("' AND NOW();");
        //println!("{}", select_string);
        retrieve_data_futures.push(async move { client.query(&select_string, &[]).await });
    }
    // execute the the data retrieval (in parallel)
    let mut sum_len = 0;
    for x in future::try_join_all(retrieve_data_futures).await? {
        //println!("retrieved data of len {}", x.len());
        sum_len += x.len()
    }
    println!(
        "Time elapsed getting {} fake data is: {:?}",
        sum_len,
        start_select.elapsed()
    );

    Ok(())
}
