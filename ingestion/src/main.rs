use bb8_postgres::PostgresConnectionManager;
use std::sync::{Arc, RwLock};
use tokio_postgres::NoTls;

use lard_ingestion::permissions;

const PARAMCONV: &str = "resources/paramconversions.csv";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("LARD ingestion service starting up...");
    // TODO: use clap for argument parsing
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        panic!(
            "USAGE: lard_ingestion <kafka_group>\nEnv vars LARD_CONN_STRING and STINFO_CONN_STRING are also needed"
            // env var format: host={} user={} dbname={} ...
        )
    }

    // Permit tables handling (needs connection to stinfosys database)
    let permit_tables = Arc::new(RwLock::new(permissions::fetch_permits().await?));
    let background_permit_tables = permit_tables.clone();

    println!("Spawing task to fetch permissions from StInfoSys...");
    // background task to refresh permit tables every 30 mins
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

        loop {
            interval.tick().await;
            async {
                // TODO: better error handling here? Nothing is listening to what returns on this task
                // but we could surface failures in metrics. Also we maybe don't want to bork the task
                // forever if these functions fail
                let new_tables = permissions::fetch_permits().await.unwrap();
                let mut tables = background_permit_tables.write().unwrap();
                *tables = new_tables;
            }
            .await;
        }
    });

    // Set up postgres connection pool
    let manager =
        PostgresConnectionManager::new_from_stringlike(std::env::var("LARD_CONN_STRING")?, NoTls)?;
    let db_pool = bb8::Pool::builder().build(manager).await?;

    // Spawn kvkafka reader
    #[cfg(feature = "kafka_prod")]
    {
        let kafka_group = args[1].to_string();
        println!("Spawing kvkafka reader...");
        tokio::spawn(lard_ingestion::kvkafka::read_and_insert(
            db_pool.clone(),
            kafka_group,
        ));
    }

    // Set up and run our server + database
    println!("Ingestion server started!");
    lard_ingestion::run(db_pool, PARAMCONV, permit_tables).await
}
