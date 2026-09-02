pub mod common;
use common::{e2e_test_setup, legacy, next, patchwork};

#[tokio::test]
async fn test_end_to_end() {
    let (producer, db_pools, permit_tables) = e2e_test_setup().await;
    eprintln!();

    futures::join!(
        next::ensure_next_ingestion_and_stations_irregular(),
        next::ensure_stations_endpoint_regular(),
        next::ensure_stations_endpoint_errors(),
        // hard to isolate since it doesn't take any param or station
        // queryparams, which it probably should. leaving it disabled for now
        // as it's more a PoC than anything
        //next::ensure_latest_endpoint(),
        next::ensure_timeslice_endpoint(),
        legacy::ensure_kafka_ingestion(producer, db_pools, permit_tables),
        patchwork::ensure_patchwork_available(),
        patchwork::ensure_patchwork(),
    );
}
