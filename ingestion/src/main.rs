use bb8_postgres::PostgresConnectionManager;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use lard_ingestion::{
    Error, FROM_TO_FUTURES_FAILURES, HTTP_REQUESTS_DURATION_SECONDS, KAFKA_CHECKED_FAILURES,
    KAFKA_CHECKED_MESSAGES_RECEIVED, KAFKA_RAW_FAILURES, KAFKA_RAW_MESSAGES_RECEIVED,
    KLDATA_FAILURES, KLDATA_MESSAGES_RECEIVED, NONSCALAR_DATAPOINTS, QC_FAILURES,
    SCALAR_DATAPOINTS, legacy,
};
use util::{
    DbPools, getenv,
    stinfofacade::{self, STINFO_CONN_STRING},
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    info!("LARD ingestion service starting up...");

    // Set up postgres connection pools
    let open_manager =
        PostgresConnectionManager::new_from_stringlike(getenv("LARD_CONN_STRING")?, NoTls)?;
    let open_db_pool = bb8::Pool::builder().build(open_manager).await?;
    let restricted_manager = PostgresConnectionManager::new_from_stringlike(
        getenv("LARD_RESTRICTED_CONN_STRING")?,
        NoTls,
    )?;
    let restricted_db_pool = bb8::Pool::builder().build(restricted_manager).await?;
    let db_pools = DbPools {
        open: open_db_pool,
        restricted: restricted_db_pool,
    };

    // set up cancellation token and signal catcher for graceful shutdown
    let cancel_token = CancellationToken::new();
    tokio::spawn(util::signal_catcher(cancel_token.clone()));

    // Setup stinfosys caches (needs connection to stinfosys database)
    let (permit_tables, permit_handle) = stinfofacade::permissions::setup_permits(
        STINFO_CONN_STRING.as_deref(),
        tokio::time::interval(tokio::time::Duration::from_secs(30 * 60)),
        cancel_token.clone(),
    )
    .await?;
    let (level_table, level_handle) = stinfofacade::level::setup_levels(
        STINFO_CONN_STRING.as_deref(),
        tokio::time::interval(tokio::time::Duration::from_secs(30 * 60)),
        cancel_token.clone(),
    )
    .await?;
    let (param_tables, param_handle) = stinfofacade::param::setup_params(
        STINFO_CONN_STRING.as_deref(),
        tokio::time::interval(tokio::time::Duration::from_secs(30 * 60)),
        cancel_token.clone(),
    )
    .await?;
    // message priority is not actually used in ingestion, we just fetch it so
    // it will be included in the stinfo backups
    let message_priority_handle = stinfofacade::message_priority::setup_refresh_message_priority(
        STINFO_CONN_STRING.as_deref(),
        tokio::time::interval(tokio::time::Duration::from_secs(30 * 60)),
        cancel_token.clone(),
    )
    .await?;
    debug!("Spawning task to refresh deactivated timeseries from StInfoSys...");
    let from_to_handle =
        tokio::task::spawn(stinfofacade::from_to_time::refresh_from_to_repeatedly(
            STINFO_CONN_STRING.as_deref(),
            level_table.clone(),
            param_tables.clone(),
            db_pools.clone(),
            tokio::time::interval(tokio::time::Duration::from_secs(6 * 3600)),
            cancel_token.clone(),
        ));

    // Set up prometheus metrics exporter
    PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(HTTP_REQUESTS_DURATION_SECONDS.to_string()),
            &[
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )
        .expect("Failed to set metric buckets")
        .install()
        .expect("Failed to set up metrics exporter");

    // Register metrics so they're guaranteed to show in exporter output
    let _ = metrics::histogram!(HTTP_REQUESTS_DURATION_SECONDS);
    let _ = metrics::counter!(KLDATA_MESSAGES_RECEIVED);
    let _ = metrics::counter!(KLDATA_FAILURES);
    let _ = metrics::counter!(QC_FAILURES);
    let _ = metrics::counter!(KAFKA_RAW_MESSAGES_RECEIVED);
    let _ = metrics::counter!(KAFKA_RAW_FAILURES);
    let _ = metrics::counter!(KAFKA_CHECKED_MESSAGES_RECEIVED);
    let _ = metrics::counter!(KAFKA_CHECKED_FAILURES);
    let _ = metrics::counter!(SCALAR_DATAPOINTS);
    let _ = metrics::counter!(NONSCALAR_DATAPOINTS);
    let _ = metrics::counter!(FROM_TO_FUTURES_FAILURES);

    // non kvalobs-dependent ingestion
    #[cfg(feature = "next")]
    let next_handle = async {
        let handle = tokio::spawn(lard_ingestion::run(
            db_pools.clone(),
            param_tables.clone(),
            permit_tables.clone(),
            level_table.clone(),
            cancel_token.clone(),
        ));

        Ok::<JoinHandle<Result<(), Error>>, Error>(handle)
    }
    .await?;

    // kvalobs-dependent ingestion
    #[cfg(feature = "legacy")]
    let legacy_handle = async {
        const KAFKA_BROKERS: &str =
    "kafka2-a1.met.no:9092, kafka2-a2.met.no:9092, kafka2-b1.met.no:9092, kafka2-b2.met.no:9092";
        const KAFKA_RAW_TOPIC: &str = "kvalobs.production.raw";
        const KAFKA_CHECKED_TOPIC: &str = "kvalobs.production.checked";
        const KAFKA_CHECKED_HIST_TOPIC: &str = "kvalobs.histkvalobs.checked";

        // TODO: use clap for argument parsing?
        let args: Vec<String> = std::env::args().collect();
        let kafka_group = args[1].clone();

        if args.len() != 2 {
            panic!(
                "USAGE: lard_ingestion <kafka_group>\n\
                Requires the following env vars:\n\
                    LARD_CONN_STRING, LARD_RESTRICTED_CONN_STRING, STINFO_CONN_STRING"
            )
        }

        let handle = tokio::spawn(legacy::run(
            db_pools,
            KAFKA_BROKERS.to_string(),
            kafka_group,
            KAFKA_RAW_TOPIC,
            KAFKA_CHECKED_TOPIC,
            KAFKA_CHECKED_HIST_TOPIC,
            cancel_token,
            permit_tables,
            level_table,
            param_tables,
        ));

        Ok::<JoinHandle<Result<(), legacy::Error>>, Error>(handle)
    }
    .await?;

    #[cfg(feature = "next")]
    next_handle.await??;
    #[cfg(feature = "legacy")]
    legacy_handle.await??;
    permit_handle.await?;
    level_handle.await?;
    param_handle.await?;
    message_priority_handle.await?;
    from_to_handle.await?;

    Ok(())
}
