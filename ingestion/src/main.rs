use bb8_postgres::PostgresConnectionManager;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use lard_ingestion::{
    cron::{self},
    get_conversions, getenv, legacy,
    util::stinfosys::Stinfosys,
    Error, FROM_TO_FUTURES_FAILURES, HTTP_REQUESTS_DURATION_SECONDS, KAFKA_CHECKED_FAILURES,
    KAFKA_CHECKED_MESSAGES_RECEIVED, KAFKA_RAW_FAILURES, KAFKA_RAW_MESSAGES_RECEIVED,
    KLDATA_FAILURES, KLDATA_MESSAGES_RECEIVED, NONSCALAR_DATAPOINTS, QC_FAILURES,
    SCALAR_DATAPOINTS,
};
use util::{stinfofacade, Cron, DbPools};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    info!("LARD ingestion service starting up...");

    let paramconv_path = getenv("PARAMCONV_CSV")?;
    let stinfo_conn_string = getenv("STINFO_CONN_STRING")?;

    // TODO: should these also accept a cancellation token?
    // Permit tables handling (needs connection to stinfosys database)
    let permit_tables = stinfofacade::permissions::setup_permits(
        // TODO: remove clone
        stinfo_conn_string.clone(),
        tokio::time::interval(tokio::time::Duration::from_secs(30 * 60)),
    )
    .await?;

    // Levels tables handling (needs connection to stinfosys database)
    let level_table = stinfofacade::level::setup_levels(
        // TODO: remove clone
        stinfo_conn_string.clone(),
        tokio::time::interval(tokio::time::Duration::from_secs(30 * 60)),
    )
    .await?;

    // set up param conversion map
    let param_conversions = get_conversions(&paramconv_path)?;

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

    debug!("Spawning task to refresh deactivated timeseries from StInfoSys...");
    tokio::task::spawn(
        Cron {
            state: (
                Stinfosys::new(stinfo_conn_string, level_table.clone()),
                db_pools.clone(),
            ),
            action: cron::refresh_from_to,
            interval: tokio::time::interval(tokio::time::Duration::from_secs(6 * 3600)),
        }
        .run_forever(),
    );

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
        use lard_ingestion::util::qc_pipelines::load_pipelines;
        use rove_connector::Connector;

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
            param_conversions.clone(),
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
            param_conversions,
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
