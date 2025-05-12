use bb8_postgres::PostgresConnectionManager;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use lard_ingestion::{
    getenv, legacy, levels, util::permissions, DbPools, Error, HTTP_REQUESTS_DURATION_SECONDS,
    KAFKA_FAILURES, KAFKA_MESSAGES_RECEIVED, KLDATA_FAILURES, KLDATA_MESSAGES_RECEIVED,
    NONSCALAR_DATAPOINTS, QC_FAILURES, SCALAR_DATAPOINTS,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    info!("LARD ingestion service starting up...");

    let stinfo_conn_string = getenv("STINFO_CONN_STRING")?;

    // Permit tables handling (needs connection to stinfosys database)
    let permit_tables = Arc::new(RwLock::new(
        permissions::fetch_permits(&stinfo_conn_string).await?,
    ));
    let background_permit_tables = permit_tables.clone();

    // Levels tables handling (needs connection to stinfosys database)
    let level_table = Arc::new(RwLock::new(
        levels::fetch_levels(&stinfo_conn_string).await?,
    ));
    let background_level_table = level_table.clone();

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

    debug!("Spawning task to fetch permissions from StInfoSys...");
    // background task to refresh permit tables every 30 mins
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

        loop {
            interval.tick().await;
            info!("Refreshing permit and level tables");
            async {
                // TODO: better error handling here? Nothing is listening to what returns on this task
                // but we could surface failures in metrics. Also we maybe don't want to bork the task
                // forever if these functions fail
                let new_permit_tables = permissions::fetch_permits(&stinfo_conn_string)
                    .await
                    .unwrap();
                let mut tables = background_permit_tables.write().unwrap();
                *tables = new_permit_tables;
            }
            .await;
            // TODO: refactor how these two tables are refreshed, since could be more elegantly combined
            // (especially if have more tables in the future)
            async {
                let new_level_table = levels::fetch_levels(&stinfo_conn_string).await.unwrap();
                let mut tables = background_level_table.write().unwrap();
                *tables = new_level_table;
            }
            .await;
        }
    });

    // set up cancellation token and signal catcher for graceful shutdown
    let cancel_token = CancellationToken::new();
    tokio::spawn(util::signal_catcher(cancel_token.clone()));

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
    let _ = metrics::counter!(KAFKA_MESSAGES_RECEIVED);
    let _ = metrics::counter!(KAFKA_FAILURES);
    let _ = metrics::counter!(SCALAR_DATAPOINTS);
    let _ = metrics::counter!(NONSCALAR_DATAPOINTS);

    // non kvalobs-dependent ingestion
    #[cfg(feature = "next")]
    let next_handle = async {
        use lard_ingestion::util::qc_pipelines::load_pipelines;
        use rove_connector::Connector;

        const PARAMCONV: &str = "resources/paramconversions.csv";

        // QC system
        // NOTE: Keeping this vesion around in case we want it for the periodic checks
        // let scheduler = rove::Scheduler::new(
        //     load_pipelines("").unwrap(),
        //     DataSwitch::new(HashMap::from([(
        //         String::from("lard"),
        //         Box::new(Connector {
        //             pool: db_pool.clone(),
        //         }) as Box<dyn DataConnector + Send>,
        //     )])),
        // );
        let rove_connector = Connector {
            pool: db_pools.open.clone(),
        };

        let qc_pipelines = load_pipelines("qc_pipelines/fresh")?;

        let handle = tokio::spawn(lard_ingestion::run(
            db_pools.clone(),
            PARAMCONV,
            permit_tables.clone(),
            level_table.clone(),
            rove_connector,
            qc_pipelines,
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
        const KAFKA_TOPIC: &str = "kvalobs.production.checked";

        // TODO: use clap for argument parsing?
        let args: Vec<String> = std::env::args().collect();
        let kafka_group = args[1].clone();

        if args.len() != 2 {
            panic!(
                "USAGE: lard_ingestion <kafka_group>\nEnv vars LARD_CONN_STRING, LARD_RESTRICTED_CONN_STRING, and STINFO_CONN_STRING are also needed"
                // env var format: host={} user={} dbname={} ...
            )
        }

        let handle = tokio::spawn(legacy::run(
            db_pools,
            KAFKA_BROKERS.to_string(),
            kafka_group,
            KAFKA_TOPIC,
            cancel_token,
            permit_tables,
            level_table,
        ));

        Ok::<JoinHandle<Result<(), legacy::Error>>, Error>(handle)
    }
    .await?;

    #[cfg(feature = "next")]
    next_handle.await??;
    #[cfg(feature = "legacy")]
    legacy_handle.await??;

    Ok(())
}
