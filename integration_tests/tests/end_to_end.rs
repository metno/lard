pub mod common;
use common::{
    calculations, e2e_test_setup, from_to_time, idf_event, legacy, next, oidc, patchwork, windrose,
};

#[tokio::test]
async fn test_end_to_end() {
    let (producer, db_pools, permit_tables, param_tables) = e2e_test_setup().await;
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
        legacy::ensure_kafka_ingestion(producer, db_pools.clone(), permit_tables),
        patchwork::ensure_patchwork_available(),
        patchwork::ensure_patchwork(),
        windrose::ensure_windrose_available(),
        windrose::ensure_windrose(),
        calculations::ensure_calculations_available(),
        calculations::ensure_calculations_specific_humidity(),
        oidc::ensure_oidc_auth(),
        idf_event::ensure_idf_event_available(),
        idf_event::ensure_idf_event(),
        from_to_time::ensure_fromtotime_update(db_pools, param_tables),
    );
}
