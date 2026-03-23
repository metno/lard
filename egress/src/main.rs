use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use bb8_postgres::PostgresConnectionManager;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use lard_egress::{
    calculations::{CALCULATIONS_AVAILABLE_REQUESTS_RECEIVED, CALCULATIONS_REQUESTS_RECEIVED},
    error::Error,
    patchwork::PatchworkTables,
    patchwork::PATCHWORK_FUTURES_FAILURES,
    reports::WINDROSE_AVAILABLE_REQUESTS_RECEIVED,
    reports::WINDROSE_REQUESTS_RECEIVED,
    PATCHWORK_AVAILABLE_REQUESTS_RECEIVED, PATCHWORK_HTTP_REQUESTS_DURATION_SECONDS,
    PATCHWORK_REQUESTS_RECEIVED,
};
use util::{getenv, stinfofacade::STINFO_CONN_STRING, DbPools};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();
    let open_connect_string = getenv("LARD_READONLY_CONN_STRING")?;
    let restricted_connect_string = getenv("LARD_READONLY_RESTRICTED_CONN_STRING")?;

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

    // set up cancellation token and signal catcher for graceful shutdown
    let cancel_token = CancellationToken::new();
    tokio::spawn(util::signal_catcher(cancel_token.clone()));

    // Patchwork handling (needs connection to stinfosys database, as well as to lard)
    // refreshes in background
    let (patchwork_tables, patchwork_handle) = PatchworkTables::setup(
        STINFO_CONN_STRING.as_deref(),
        db_pools.clone(),
        tokio::time::interval(tokio::time::Duration::from_secs(30 * 60)),
        cancel_token.clone(),
    )
    .await?;

    // Cache the public key for checking tokens
    debug!("Caching the public key for authentication...");
    let auth_certs = lard_egress::auth::cache_jwks_certs().await?;

    // Set up S3 bucket for IDF
    let bucket = Arc::from(
        s3::Bucket::new(
            &getenv("S3_BUCKET_NAME")?,
            s3::Region::from_env("AWS_REGION", Some("S3_ENDPOINT_URL")).unwrap(),
            // Requires "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" to be set
            // it's a bit cursed the API treats these differently
            s3::creds::Credentials::from_env().unwrap(),
        )?
        .with_path_style(),
    );

    // Set up prometheus metrics exporter
    // on a different port than the default 9000, since that is used in ingestion
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9003);
    PrometheusBuilder::new()
        .with_http_listener(socket)
        .set_buckets_for_metric(
            Matcher::Full(PATCHWORK_HTTP_REQUESTS_DURATION_SECONDS.to_string()),
            &[
                0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ],
        )
        .expect("Failed to set metric buckets")
        .install()
        .expect("Failed to set up metrics exporter");

    // Register metrics so they're guaranteed to show in exporter output
    let _ = metrics::histogram!(PATCHWORK_HTTP_REQUESTS_DURATION_SECONDS);
    let _ = metrics::counter!(PATCHWORK_FUTURES_FAILURES);
    let _ = metrics::counter!(PATCHWORK_AVAILABLE_REQUESTS_RECEIVED);
    let _ = metrics::counter!(PATCHWORK_REQUESTS_RECEIVED);
    let _ = metrics::counter!(WINDROSE_AVAILABLE_REQUESTS_RECEIVED);
    let _ = metrics::counter!(WINDROSE_REQUESTS_RECEIVED);
    let _ = metrics::counter!(CALCULATIONS_AVAILABLE_REQUESTS_RECEIVED);
    let _ = metrics::counter!(CALCULATIONS_REQUESTS_RECEIVED);

    let egress_handle = tokio::spawn(lard_egress::run(
        db_pools.clone(),
        Some(bucket),
        patchwork_tables,
        auth_certs,
        cancel_token.clone(),
    ));
    egress_handle.await?;
    patchwork_handle.await?;

    Ok(())
}
