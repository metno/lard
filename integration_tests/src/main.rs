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

async fn add_files_to_bucket() {
    let bucket = s3::Bucket::new(
        &std::env::var("S3_BUCKET_NAME").unwrap(),
        s3::Region::Custom {
            region: std::env::var("AWS_REGION").unwrap(),
            endpoint: std::env::var("S3_ENDPOINT_URL").unwrap(),
        },
        // Requires "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" to be set
        s3::creds::Credentials::from_env().unwrap(),
    )
    .unwrap()
    // TODO: not sure what the path would be otherwise
    .with_path_style();

    let files = [
        (
            "/metadata.csv",
            "12345,39,1968,2023,3,0,2024-01-01
67890,50,1999,2009,0,0,2010-01-01",
        ),
        (
            "/12345.csv",
            "12345,39,1968,2023,3,0,2024-01-01
1,1,1.5,1.2,1.7
1,2,1.5,1.2,1.7
2,1,1.5,1.2,1.7
2,2,1.5,1.2,1.7",
        ),
    ];

    for (path, content) in files {
        if let Err(e) = bucket.put_object(path, content.as_bytes()).await {
            panic!("{e}")
        };
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
    add_files_to_bucket().await;
}
