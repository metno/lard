use std::fs;

use tokio_postgres::{Error, NoTls};

const CONNECT_STRING_POSTGRES: &str =
    "host=localhost user=postgres dbname=postgres password=postgres";
const CONNECT_STRING_LARD: &str = "host=localhost user=postgres dbname=lard password=postgres";
const CONNECT_STRING_LARD_RESTRICTED: &str =
    "host=localhost user=postgres dbname=lard_restricted password=postgres";

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
    let (postgres_client, connection) = tokio_postgres::connect(CONNECT_STRING_POSTGRES, NoTls)
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

    for conn_string in [CONNECT_STRING_LARD, CONNECT_STRING_LARD_RESTRICTED] {
        let (client, connection) = tokio_postgres::connect(conn_string, NoTls)
            .await
            .expect("Should be able to connect to database");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {e}");
            }
        });

        for file in files {
            let statements = file.to_str().unwrap();
            insert_schema(&client, statements).await.expect(statements);
        }
    }
}
