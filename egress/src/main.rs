use std::sync::{Arc, RwLock};

use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use lard_egress::{error::Error, filter};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // set up postgres connection pool
    let connect_string = std::env::var("LARD_CONN_STRING")?;
    let manager = PostgresConnectionManager::new_from_stringlike(connect_string, NoTls)?;
    let pool = bb8::Pool::builder().build(manager).await?;

    let stinfo_conn_string = std::env::var("STINFO_CONN_STRING")?;

    // Filter handling (needs connection to stinfosys database, as well as to lard)
    let conn = pool.get().await?;
    let filter_table = Arc::new(RwLock::new(
        filter::create_filter_table_wrapper(&conn, &stinfo_conn_string).await?,
    ));
    let background_filter_table = filter_table.clone();

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
                let new_filter_table =
                    filter::create_filter_table_wrapper(conn_loop, &stinfo_conn_string)
                        .await
                        .unwrap();
                let mut table = background_filter_table.write().unwrap();
                *table = new_filter_table;
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

    tokio::spawn(lard_egress::run(
        pool.clone(),
        bucket,
        filter_table,
        cancel_token.clone(),
    ));

    Ok(())
}
