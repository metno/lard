use std::fs;

use tokio_postgres::{Error, NoTls};

async fn insert_schema(client: &tokio_postgres::Client, filename: &str) -> Result<(), Error> {
    let schema = fs::read_to_string(filename).expect("Should be able to read SQL file");
    client.batch_execute(schema.as_str()).await
}

fn parse_database_directory() -> Vec<std::path::PathBuf> {
    let mut files: Vec<_> = fs::read_dir("db")
        .unwrap()
        .map(|res| res.unwrap())
        // Only applying files whose name starts with 3 digits,
        // so that they can be properly sorted
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .unwrap()
                .bytes()
                .take(3)
                .all(|b| b.is_ascii_digit())
        })
        .map(|entry| entry.path())
        .collect();

    files.sort();

    files
}

#[tokio::main]
async fn main() {
    let (postgres_client, connection) =
        tokio_postgres::connect(&std::env::var("PG_CONN_STRING").unwrap(), NoTls)
            .await
            .expect("Should be able to connect to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    postgres_client
        .execute("CREATE DATABASE lard", &[])
        .await
        .expect("Failed to create lard db");
    postgres_client
        .execute("CREATE DATABASE lard_restricted", &[])
        .await
        .expect("Failed to create lard_restricted db");

    let files = parse_database_directory();

    for conn_string in [
        &std::env::var("LARD_CONN_STRING").unwrap(),
        &std::env::var("LARD_RESTRICTED_CONN_STRING").unwrap(),
    ] {
        let (client, connection) = tokio_postgres::connect(conn_string, NoTls)
            .await
            .expect("Should be able to connect to database");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {e}");
            }
        });

        for file in files.iter() {
            let statements = file.to_str().unwrap();
            insert_schema(&client, statements).await.expect(statements);
        }

        // hack to put data into db if running the frost integration docker compose setup
        match &std::env::var("FROST_DATA") {
            Ok(val) => {
                if val == "true" && !conn_string.contains("restricted") {
                    let tsid: i64 = 1234;
                    let stnid: i32 = 18700;
                    let paramid: i32 = 211;
                    let typeid: i32 = 506;
                    let lvl: i32 = 200;
                    let sensor: i32 = 0;
                    let permit: i32 = 1;
                    let value: f64 = 12.03;
                    client
                        .execute("INSERT INTO public.timeseries (id, fromtime, permit) VALUES ($1, now()::DATE - 1, $2)", &[&tsid, &permit])
                        .await
                        .expect("Failed to insert timeseries");
                    client
                        .execute("INSERT INTO labels.met (timeseries, station_id, param_id, type_id, lvl, sensor) VALUES ($1, $2, $3, $4, $5, $6)", &[&tsid, &stnid, &paramid, &typeid, &lvl, &sensor])
                        .await
                        .expect("Failed to insert label");
                    client
                        .execute("INSERT INTO legacy.data (timeseries, obstime, original, corrected) VALUES ($1, NOW(), $2, $3)", &[&tsid, &value, &value])
                        .await
                        .expect("Failed to insert label");
                }
            }
            Err(e) => println!("Did not find environment variable FROST_DATA {}", e),
        }
    }
}
