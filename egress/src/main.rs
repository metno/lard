use std::sync::{Arc, RwLock};

use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use lard_egress::{
    error::Error,
    filter::{
        create_filter_timeseries_table, fetch_message_priority_default,
        fetch_message_priority_exception, fetch_timeseries_list_from_database,
    },
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // set up postgres connection pool
    let connect_string = std::env::var("LARD_CONN_STRING")?;
    let manager = PostgresConnectionManager::new_from_stringlike(connect_string, NoTls)?;
    let pool = bb8::Pool::builder().build(manager).await?;

    let stinfo_conn_string = std::env::var("STINFO_CONN_STRING")?;

    let db_ts_list = fetch_timeseries_list_from_database(&pool.get().await.unwrap()).await?;
    let default_table = Arc::new(RwLock::new(
        fetch_message_priority_default(&stinfo_conn_string).await?,
    ));
    let exception_table = Arc::new(RwLock::new(
        fetch_message_priority_exception(&stinfo_conn_string).await?,
    ));

    // Filter handling (needs connection to stinfosys database, as well as to lard)
    let filter_list = Arc::new(RwLock::new(
        create_filter_timeseries_table(db_ts_list, default_table, exception_table).unwrap(),
    ));
    let mut background_filter_list = filter_list.clone();

    let pool_loop = pool.clone();
    debug!("Spawning task to refresh filter table...");
    // background task to refresh filter table every 30 mins
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

        loop {
            interval.tick().await;
            info!("Refreshing filter table");
            let conn_loop = &pool_loop.get().await.unwrap();
            async {
                let new_db_ts_list = fetch_timeseries_list_from_database(conn_loop)
                    .await
                    .unwrap();
                let new_default_table = Arc::new(RwLock::new(
                    fetch_message_priority_default(&stinfo_conn_string)
                        .await
                        .unwrap(),
                ));
                let new_exception_table = Arc::new(RwLock::new(
                    fetch_message_priority_exception(&stinfo_conn_string)
                        .await
                        .unwrap(),
                ));
                let new_filter_list = Arc::new(RwLock::new(
                    create_filter_timeseries_table(
                        new_db_ts_list,
                        new_default_table,
                        new_exception_table,
                    )
                    .unwrap(),
                ));
                background_filter_list = new_filter_list;
            }
            .await;
        }
    });

    // Set up S3 bucket for IDF
    let bucket = Arc::from(
        s3::Bucket::new(
            &std::env::var("S3_BUCKET_NAME")?,
            s3::Region::from_env("AWS_REGION", Some("S3_ENDPOINT_URL")).unwrap(),
            // Requires "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" to be set
            // it's a bit cursed the API treats these differently
            s3::creds::Credentials::from_env().unwrap(),
        )?
        .with_path_style(),
    );

    // set up cancellation token and signal catcher for graceful shutdown
    let cancel_token = CancellationToken::new();
    tokio::spawn(util::signal_catcher(cancel_token.clone()));

    tokio::spawn(lard_egress::run(pool, bucket, cancel_token.clone()));

    Ok(())
}
