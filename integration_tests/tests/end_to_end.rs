pub mod common;
use common::{e2e_test_setup, next};

#[tokio::test]
async fn test_end_to_end() {
    let _db_pools = e2e_test_setup().await;
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
    );
}
