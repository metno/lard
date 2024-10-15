use std::fs;

use tokio_postgres::{Error, NoTls};

const CONNECT_STRING: &str = "host=localhost user=postgres dbname=postgres password=postgres";

async fn insert_schema(client: &tokio_postgres::Client, filename: &str) -> Result<(), Error> {
    let schema = fs::read_to_string(filename).expect("Should be able to read SQL file");
    client.batch_execute(schema.as_str()).await
}

fn format_partition(start: &str, end: &str, table: &str) -> String {
    // TODO: add multiple partitions?
    format!(
        "CREATE TABLE {table}_y{start}_to_y{end} PARTITION OF {table} \
        FOR VALUES FROM ('{start}-01-01 00:00:00+00') TO ('{end}-01-01 00:00:00+00')",
    )
}

async fn create_data_partitions(client: &tokio_postgres::Client) -> Result<(), Error> {
    let scalar_string = format_partition("1950", "2100", "public.data");
    let nonscalar_string = format_partition("1950", "2100", "public.nonscalar_data");

    client.batch_execute(scalar_string.as_str()).await?;
    client.batch_execute(nonscalar_string.as_str()).await
}

#[tokio::main]
async fn main() {
    let (client, connection) = tokio_postgres::connect(CONNECT_STRING, NoTls)
        .await
        .expect("Should be able to connect to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // NOTE: order matters
    let schemas = ["db/public.sql", "db/labels.sql", "db/flags.sql"];
    for schema in schemas {
        insert_schema(&client, schema).await.unwrap();
    }

    create_data_partitions(&client).await.unwrap();
}
