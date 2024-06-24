use std::fs;

use tokio_postgres::{Error, NoTls};

const CONNECT_STRING: &str = "host=localhost user=postgres dbname=postgres password=postgres";

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (client, connection) = tokio_postgres::connect(CONNECT_STRING, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let public_schema =
        fs::read_to_string("db/public.sql").expect("Should be able to read SQL file");
    client.batch_execute(public_schema.as_str()).await?;

    let labels_schema =
        fs::read_to_string("db/labels.sql").expect("Should be able to read SQL file");
    client.batch_execute(labels_schema.as_str()).await?;

    // TODO: add multiple partitions?
    let partition_string = format!(
        "CREATE TABLE data_y{}_to_y{} PARTITION OF public.data FOR VALUES FROM ('{}') TO ('{}')",
        "1950", "2100", "1950-01-01 00:00:00+00", "2100-01-01 00:00:00+00",
    );
    client.batch_execute(partition_string.as_str()).await
}
