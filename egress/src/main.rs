use std::sync::{Arc, RwLock};

use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use lard_egress::{
    error::Error,
    patchwork::{self, PatchworkTimeseriesTables},
};
use util::DbPools;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let open_connect_string = std::env::var("LARD_CONN_STRING")?;
    let restricted_connect_string = std::env::var("LARD_RESTRICTED_CONN_STRING")?;

    // Set up postgres connection pools
    let open_manager = PostgresConnectionManager::new_from_stringlike(open_connect_string, NoTls)?;
    let open_db_pool = bb8::Pool::builder().build(open_manager).await?;
    let restricted_manager =
        PostgresConnectionManager::new_from_stringlike(restricted_connect_string, NoTls)?;
    let restricted_db_pool = bb8::Pool::builder().build(restricted_manager).await?;
    let db_pools = DbPools {
        open: open_db_pool,
        restricted: restricted_db_pool,
    };

    // get stinfo conn
    let stinfo_conn_string = std::env::var("STINFO_CONN_STRING")?;
    let (stinfosys_client, stinfosys_conn) =
        tokio_postgres::connect(&stinfo_conn_string, NoTls).await?;
    // conn object independently performs communication with database, so needs it's own task.
    // it will return when the client is dropped
    tokio::spawn(async move {
        if let Err(e) = stinfosys_conn.await {
            error!("connection error: {}", e);
        }
    });

    // Patchwork handling (needs connection to stinfosys database, as well as to lard)
    let open_conn = db_pools.open.get().await?;
    let patchwork_table_open =
        patchwork::fetch_patchwork_table(&open_conn, &stinfosys_client).await?;

    let restricted_conn = db_pools.restricted.get().await?;
    let patchwork_table_restricted =
        patchwork::fetch_patchwork_table(&restricted_conn, &stinfosys_client).await?;
    let background_patchwork_tables = Arc::new(RwLock::new((
        patchwork_table_open.clone(),
        patchwork_table_restricted.clone(),
    )));

    // Cache the public key for checking tokens
    let auth_certs = lard_egress::auth::cache_jwks_certs().await?;

    let pool_loop = db_pools.clone();
    debug!("Spawning task to refresh patchwork table...");
    // background task to refresh patchwork table every 30 mins
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

        loop {
            interval.tick().await;
            info!("Refreshing patchwork table");
            let open_conn_loop = &pool_loop.open.get().await.unwrap();
            let restricted_conn_loop = &pool_loop.restricted.get().await.unwrap();
            async {
                let new_open_patchwork_table =
                    patchwork::fetch_patchwork_table(open_conn_loop, &stinfosys_client)
                        .await
                        .unwrap();
                let new_restricted_patchwork_table =
                    patchwork::fetch_patchwork_table(restricted_conn_loop, &stinfosys_client)
                        .await
                        .unwrap();
                let mut table = background_patchwork_tables.write().unwrap();
                table.0 = new_open_patchwork_table;
                table.1 = new_restricted_patchwork_table;
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

    let patchwork_tables =
        PatchworkTimeseriesTables::new(patchwork_table_open, patchwork_table_restricted);

    let egress_handle = tokio::spawn(lard_egress::run(
        db_pools.clone(),
        bucket,
        patchwork_tables,
        auth_certs,
        cancel_token.clone(),
    ));
    egress_handle.await?;

    Ok(())
}
