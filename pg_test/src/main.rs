use std::env;
use tokio_postgres::{Error, NoTls};

#[derive(Debug)]
struct Location {
    _lat: f32,
    _lon: f32,
    _hamsl: f32,
    _hag: f32,
}

#[derive(Debug)]
struct FilterLabel {
    _stn: f32,
    _elem: String,
    _lvl: i32,
    _sensor: i32,
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
    println!("{}", connect_string);

    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(connect_string.as_str(), NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    for row in client
        .query(
            "SELECT timeseries.id, (timeseries.loc).lat, (timeseries.loc).lon, (timeseries.loc).hamsl, (timeseries.loc).hag, (filter.label).stationID, (filter.label).elementID, (filter.label).lvel, (filter.label).sensor FROM public.timeseries 
            JOIN labels.filter
            ON timeseries.id = labels.filter.timeseries;",
            &[],
        )
        .await?
    {
        let id: i32 = row.get(0);
        let loc = Location {
            _lat: row.get(1),
            _lon: row.get(2),
            _hamsl: row.get(3),
            _hag: row.get(4),
        };
        let label = FilterLabel {
            _stn: row.get(5),
            _elem: row.get(6),
            _lvl: row.get(7),
            _sensor: row.get(8),
        };

        println!("{} {:?} {:?}", id, loc, label);
    }

    Ok(())
}
