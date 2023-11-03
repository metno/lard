use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use futures::pin_mut;
use rand::{seq::SliceRandom, Rng};
use rand_distr::{Distribution, Poisson};
use std::{env, fs, time::Instant};
use tokio_postgres::{
    types::{FromSql, ToSql, Type},
    NoTls,
};

const ELEMENTS: &[&str] = &["air_temperature", "precipitation", "wind_speed"];

#[derive(Debug, ToSql, FromSql)]
struct Data {
    timeseries: i32,
    obstime: DateTime<Utc>,
    obsvalue: f32,
}

struct TimeseriesSpec {
    id: i32,
    start_time: DateTime<Utc>,
    period: Duration,
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

async fn create_timeseries(
    client: &tokio_postgres::Client,
    n_timeseries: usize,
    mean_timeseries_length: usize,
) -> Result<Vec<TimeseriesSpec>, tokio_postgres::Error> {
    // create rand
    let mut rng = rand::thread_rng();
    let poisson = Poisson::new(mean_timeseries_length as f32).unwrap();
    // keep list of tsids, with period and start time
    let mut timeseries = Vec::new();

    // insert a bunch of timeseries
    for _ in 0..n_timeseries {
        // Generate ts series length according to poisson distribution
        let ts_length: i32 = poisson.sample(&mut rng) as i32;

        // Randomise period between daily, hourly, and minutely data.
        // + calculate start time from period and ts_length
        let (period, start_time) = match rng.gen_range(0..3) {
            0 => {
                let period = Duration::days(1);
                let now = Utc::now();
                let start_time = Utc
                    .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
                    .unwrap()
                    - (period * ts_length);
                (period, start_time)
            }
            1 => {
                let period = Duration::hours(1);
                let now = Utc::now();
                let start_time = Utc
                    .with_ymd_and_hms(now.year(), now.month(), now.day(), now.hour(), 0, 0)
                    .unwrap()
                    - (period * ts_length);
                (period, start_time)
            }
            2 => {
                let period = Duration::minutes(1);
                let now = Utc::now();
                let start_time = Utc
                    .with_ymd_and_hms(
                        now.year(),
                        now.month(),
                        now.day(),
                        now.hour(),
                        now.minute(),
                        0,
                    )
                    .unwrap()
                    - (period * ts_length.try_into().unwrap());
                (period, start_time)
            }
            _ => unreachable!(),
        };

        let random_lat = rng.gen_range(59..72) as f32 * 0.5;
        let random_lon = rng.gen_range(4..30) as f32 * 0.5;

        let tsid: i32 = client.query(
            "INSERT INTO timeseries (id, fromtime, loc.lat, loc.lon, deactivated) VALUES(DEFAULT, $1, $2, $3, false) RETURNING id",
            &[&start_time, &random_lat, &random_lon],
        ).await?.first().unwrap().get(0);

        timeseries.push(TimeseriesSpec {
            id: tsid,
            start_time,
            period,
        });

        // also label the timeseries
        let random_station_id = rng.gen_range(1000..2000) as f32;
        let random_element_id = ELEMENTS.choose(&mut rng).unwrap();
        let level: i32 = 0;
        let sensor: i32 = 0;

        client.execute(
            "INSERT INTO labels.filter (timeseries, label.stationID, label.elementID, label.lvl, label.sensor) VALUES($1, $2, $3, $4, $5)",
            &[&tsid, &random_station_id, &random_element_id, &level, &sensor],
        ).await?;
    }
    Ok(timeseries)
}

fn create_data_vec(timeseries_vec: &Vec<TimeseriesSpec>) -> Vec<[Box<dyn ToSql + Sync>; 3]> {
    let mut data_vec: Vec<[Box<dyn ToSql + Sync>; 3]> = Vec::new();
    let mut rng = rand::thread_rng();
    for ts in timeseries_vec {
        // insert hourly data from the past data until now
        let mut time = ts.start_time;
        let now = Utc::now();
        let round_now: DateTime<Utc> = Utc
            .with_ymd_and_hms(now.year(), now.month(), now.day(), now.hour(), 0, 0)
            .unwrap();
        while time <= round_now {
            let v = rng.gen_range(0..30) as f32 * 0.5;
            data_vec.push([Box::new(ts.id), Box::new(time), Box::new(v)]);

            time += ts.period;
        }
    }

    data_vec
}

async fn remove_constraints_and_indices(
    client: &tokio_postgres::Client,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "ALTER TABLE public.data DROP CONSTRAINT unique_data_timeseries_obstime",
            &[],
        )
        .await?;
    client
        .execute(
            "ALTER TABLE public.data DROP CONSTRAINT fk_data_timeseries",
            &[],
        )
        .await?;
    client
        .execute("DROP INDEX timestamp_data_index", &[])
        .await?;
    client
        .execute("DROP INDEX timeseries_data_index", &[])
        .await?;
    Ok(())
}

async fn add_constraints_and_indices(
    client: &tokio_postgres::Client,
) -> Result<(), tokio_postgres::Error> {
    println!("Adding unique constraint...");
    client
        .execute(
            "ALTER TABLE public.data ADD CONSTRAINT unique_data_timeseries_obstime UNIQUE (timeseries, obstime)",
            &[],
        )
        .await?;
    println!("Adding foreign key constraint...");
    client
        .execute(
            "ALTER TABLE public.data ADD CONSTRAINT fk_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries",
            &[],
        )
        .await?;
    println!("Adding timestamp index...");
    client
        .execute(
            "CREATE INDEX timestamp_data_index ON public.data (obstime)",
            &[],
        )
        .await?;
    println!("Adding timeseries index...");
    client
        .execute(
            "CREATE INDEX timeseries_data_index ON public.data USING HASH (timeseries)",
            &[],
        )
        .await?;
    Ok(())
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
    let ts_vec = create_timeseries(&client, 10, 10).await?;

    // create data
    println!("Making fake data...");
    let data_vec = create_data_vec(&ts_vec);

    println!("Removing constraints and indices...");
    remove_constraints_and_indices(&client).await?;

    println!("Copy in data...");
    copy_in_data(&mut client, data_vec).await?;

    println!("Adding constraints and indices...");
    add_constraints_and_indices(&client).await?;

    Ok(())
}
