pub mod common;
use common::{
    calculations, e2e_test_setup, from_to_time, idf_event, legacy, next, oidc, patchwork,
    time_resolution, windrose,
};

#[tokio::test]
/// Instead of individual tests, which would need to run single-threaded and
/// have their whole environment (dq, queues, etc) spun up on and torn down,
/// we run cases in parallel on a pre-populated db. This is a lot faster.
///
/// If you are considering writing an integration test, please consider if it
/// can be written as a case here instead.
///
/// To write a new case:
/// - create any data or metadata you need preloaded in
///   `lard/resources/mock_content/end_to_end_test`.
///   see [`util::mock::data`] for context on the mock data format, and
///   [`util::stinfofacade::persistence`] for context on the mock metadata
///   format. Try to reuse what already exists in the dataset if possible, to
///   keep our configuration simple.
/// - write an async fn under [`common`] (in a new module if it doesn't fit
///   into an existing one) that produces the behaviour you want to test.
/// - include `eprintln!("<case name> ok")` at the end of your function, so
///   the user can see when it has succeeded.
/// - call your function in the `join!` call below (but don't await it, the
///   join itself handles that).
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
        from_to_time::ensure_fromtotime_update(db_pools.clone(), param_tables),
        time_resolution::ensure_time_resolution(db_pools),
    );
}
