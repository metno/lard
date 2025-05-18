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

async fn create_s3_bucket() {
    let resp = s3::Bucket::create(
        &std::env::var("S3_BUCKET_NAME").unwrap(),
        s3::Region::Custom {
            region: std::env::var("AWS_REGION").unwrap(),
            endpoint: std::env::var("S3_ENDPOINT_URL").unwrap(),
        },
        // Requires "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" to be set
        s3::creds::Credentials::from_env().unwrap(),
        s3::BucketConfiguration::default(),
    )
    .await
    .unwrap();

    if !resp.success() {
        panic!("Bucket could not be created")
    }
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
        &std::env::var("LARD_CONN_STRING_RESTRICTED").unwrap(),
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
    }

    // Setup S3 bucket for IDF
    create_s3_bucket().await;
}
