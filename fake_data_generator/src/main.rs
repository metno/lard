use chrono::{DateTime, Datelike, Duration, Months, TimeZone, Timelike, Utc};
use futures::pin_mut;
use rand::{
    // seq::SliceRandom,
    Rng,
};
use rand_distr::{Distribution, Geometric};
use std::{env, fs, time::Instant};
use tokio_postgres::{
    types::{FromSql, ToSql, Type},
    NoTls,
};

// const ELEMENTS: &[&str] = &["air_temperature", "precipitation", "wind_speed"];

#[derive(Debug, ToSql, FromSql)]
struct Data {
    timeseries: i32,
    obstime: DateTime<Utc>,
    obsvalue: f32,
}

struct TimeseriesSpec {
    id: i32,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    period: Duration,
}

async fn create_data_partitions(
    client: &tokio_postgres::Client,
) -> Result<(), tokio_postgres::Error> {
    // create a vector of the boundaries between partitions
    let paritition_boundary_years: Vec<DateTime<Utc>> = [1950, 2000, 2010]
        .into_iter()
        .chain(2015..=2030)
        .map(|y| Utc.with_ymd_and_hms(y, 1, 1, 0, 0, 0).unwrap())
        .collect();

    // .windows(2) gives a 2-wide sliding view of the vector, so we can see
    // both bounds relevant to a partition
    for window in paritition_boundary_years.windows(2) {
        let start_time = window[0];
        let end_time = window[1];

        let query_string = format!(
            "CREATE TABLE data_y{}_to_y{} PARTITION OF public.data FOR VALUES FROM ('{}') TO ('{}')",
            start_time.format("%Y"),
            end_time.format("%Y"),
            start_time.format("%Y-%m-%d %H:%M:%S+00"),
            end_time.format("%Y-%m-%d %H:%M:%S+00")
        );
        client.execute(&query_string, &[]).await?;
    }

    Ok(())
}

async fn cleanup_setup(client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    // cleanup stuff before?
    client
        .execute(
            "DROP TABLE IF EXISTS timeseries, data, labels.met CASCADE",
            &[],
        )
        .await?;
    client.execute("DROP TYPE IF EXISTS location", &[]).await?;

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

    println!("Creating partitions...");
    create_data_partitions(client).await?;

    Ok(())
}

async fn create_timeseries(
    client: &tokio_postgres::Client,
    n_timeseries: usize,
    mean_timeseries_length: usize,
) -> Result<Vec<TimeseriesSpec>, tokio_postgres::Error> {
    // create rand
    let mut rng = rand::thread_rng();
    let length_geometric = Geometric::new(1_f64 / mean_timeseries_length as f64).unwrap();
    let age_geometric = Geometric::new(0.2).unwrap();
    // keep list of tsids, with period and start time
    let mut timeseries = Vec::new();

    // insert a bunch of timeseries
    for i in 0..n_timeseries {
        // Generate ts series length according to poisson distribution
        let ts_length: i32 = length_geometric.sample(&mut rng) as i32;
        let present_time = Utc.with_ymd_and_hms(2023, 11, 3, 23, 9, 0).unwrap();

        // Randomise period between daily, hourly, and minutely data.
        // + calculate start time from period and ts_length
        let (period, mut start_time, end_time) = match rng.gen_range(0..3) {
            0 => {
                let period = Duration::days(1);
                let start_time = Utc
                    .with_ymd_and_hms(
                        present_time.year(),
                        present_time.month(),
                        present_time.day(),
                        0,
                        0,
                        0,
                    )
                    .unwrap()
                    - (period * ts_length);
                (period, start_time, present_time)
            }
            1 => {
                let period = Duration::hours(1);
                let year_skew = Months::new(12 * age_geometric.sample(&mut rng) as u32);
                let start_time = Utc
                    .with_ymd_and_hms(
                        present_time.year(),
                        present_time.month(),
                        present_time.day(),
                        present_time.hour(),
                        0,
                        0,
                    )
                    .unwrap()
                    - (period * ts_length)
                    - year_skew;
                let end_time = present_time - year_skew;
                (period, start_time, end_time)
            }
            2 => {
                let period = Duration::minutes(1);
                let year_skew = Months::new(12 * age_geometric.sample(&mut rng) as u32);
                let start_time = Utc
                    .with_ymd_and_hms(
                        present_time.year(),
                        present_time.month(),
                        present_time.day(),
                        present_time.hour(),
                        present_time.minute(),
                        0,
                    )
                    .unwrap()
                    - (period * ts_length)
                    - year_skew;
                let end_time = present_time - year_skew;
                (period, start_time, end_time)
            }
            _ => unreachable!(),
        };

        if start_time < Utc.with_ymd_and_hms(1950, 1, 1, 0, 0, 0).unwrap() {
            start_time = Utc.with_ymd_and_hms(1950, 1, 1, 0, 0, 0).unwrap();
        }

        let random_lat = rng.gen_range(59..72) as f32 * 0.5;
        let random_lon = rng.gen_range(4..30) as f32 * 0.5;

        let tsid: i32 = client.query(
            "INSERT INTO timeseries (id, fromtime, loc.lat, loc.lon, deactivated) VALUES(DEFAULT, $1, $2, $3, false) RETURNING id",
            &[&start_time, &random_lat, &random_lon],
        ).await?.first().unwrap().get(0);

        timeseries.push(TimeseriesSpec {
            id: tsid,
            start_time,
            end_time,
            period,
        });

        // also label the timeseries
        // TODO: smarter generation strategy to avoid duplicates
        let random_station_id = rng.gen_range(1000..2000);
        let random_param_id = rng.gen_range(1000..2000);
        let random_type_id = rng.gen_range(1000..2000);
        // let random_element_id = ELEMENTS.choose(&mut rng).unwrap();
        let level: i32 = 0;
        let sensor: i32 = 0;

        client.execute(
            "INSERT INTO labels.met (timeseries, station_id, param_id, type_id, lvl, sensor) VALUES($1, $2, $3, $4, $5, $6)",
            &[&tsid, &random_station_id, &random_param_id, &random_type_id, &level, &sensor],
        ).await?;

        print!("\r{}/{}", i, n_timeseries);
    }
    println!();
    Ok(timeseries)
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
        .execute("DROP INDEX data_timestamp_index", &[])
        .await?;
    client
        .execute("DROP INDEX data_timeseries_index", &[])
        .await?;
    Ok(())
}

async fn add_constraints_and_indices(
    client: &tokio_postgres::Client,
) -> Result<(), tokio_postgres::Error> {
    println!("Adding unique constraint...");
    let unique_start = Instant::now();
    client
        .execute(
            "ALTER TABLE public.data ADD CONSTRAINT unique_data_timeseries_obstime UNIQUE (timeseries, obstime)",
            &[],
        )
        .await?;
    println!("took: {:?}", unique_start.elapsed());

    println!("Adding foreign key constraint...");
    let fk_start = Instant::now();
    client
        .execute(
            "ALTER TABLE public.data ADD CONSTRAINT fk_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries",
            &[],
        )
        .await?;
    println!("took: {:?}", fk_start.elapsed());

    println!("Adding timestamp index...");
    let timestamp_start = Instant::now();
    client
        .execute(
            "CREATE INDEX data_timestamp_index ON public.data (obstime)",
            &[],
        )
        .await?;
    println!("took: {:?}", timestamp_start.elapsed());

    println!("Adding timeseries index...");
    let timeseries_start = Instant::now();
    client
        .execute(
            "CREATE INDEX data_timeseries_index ON public.data USING HASH (timeseries)",
            &[],
        )
        .await?;
    println!("took: {:?}", timeseries_start.elapsed());

    println!("Vacuuming...");
    let vacuum_start = Instant::now();
    client.execute("VACUUM ANALYZE", &[]).await?;
    println!("took: {:?}", vacuum_start.elapsed());

    Ok(())
}

async fn copy_in_data(
    client: &mut tokio_postgres::Client,
    timeseries_vec: Vec<TimeseriesSpec>,
) -> Result<(), tokio_postgres::Error> {
    let start_copy_in = Instant::now();

    let tx = client.transaction().await?;
    let sink = tx.copy_in("COPY data FROM STDIN BINARY").await?;
    let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(
        sink,
        &[Type::INT4, Type::TIMESTAMPTZ, Type::FLOAT4],
    );
    pin_mut!(writer);

    let mut num_rows_inserted = 0;

    let mut rng = rand::thread_rng();
    for i in 0..timeseries_vec.len() {
        let ts = &timeseries_vec[i];
        // insert hourly data from the past data until now
        let mut time = ts.start_time;
        while time <= ts.end_time {
            let v = rng.gen_range(0..30) as f32 * 0.5;

            writer.as_mut().write(&[&ts.id, &time, &v]).await?;

            time += ts.period;
            num_rows_inserted += 1
        }
        print!("\r{}/{}", i, timeseries_vec.len());
    }
    println!();

    writer.finish().await?;
    tx.commit().await?;

    println!(
        "Time elapsed copying {} fake data is: {:?}",
        num_rows_inserted,
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
        connect_string.push_str(" password=");
        connect_string.push_str(&args[4])
    }

    // Connect to the database.
    let (mut client, connection) = tokio_postgres::connect(connect_string.as_str(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let populate_start = Instant::now();

    // clear things, and setup again
    cleanup_setup(&client).await?;

    // create random timeseries
    println!("Creating timeseries...");
    let ts_vec = create_timeseries(&client, 100000, 10000).await?;

    println!("Removing constraints and indices...");
    remove_constraints_and_indices(&client).await?;

    println!("Copy in data...");
    copy_in_data(&mut client, ts_vec).await?;

    println!("Adding constraints and indices...");
    add_constraints_and_indices(&client).await?;

    println!("Time elapsed total is: {:?}", populate_start.elapsed());

    Ok(())
}
