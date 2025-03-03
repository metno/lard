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

    for conn_string in [CONNECT_STRING_LARD, CONNECT_STRING_LARD_RESTRICTED] {
        let (client, connection) = tokio_postgres::connect(conn_string, NoTls)
            .await
            .expect("Should be able to connect to database");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {e}");
            }
        });

        // NOTE: order matters
        let schemas = [
            "db/public.sql",
            "db/labels.sql",
            "db/flags.sql",
            "db/partitions_generated.sql",
        ];
        for schema in schemas {
            insert_schema(&client, schema).await.unwrap();
        }
    }
}
