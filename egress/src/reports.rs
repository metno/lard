use axum::{routing::get, Router};

use crate::EgressState;

mod idf_event;
use idf_event::{idf_event_availability_handler, idf_event_handler};

mod idf_station;
use idf_station::{idf_station_availability_handler, idf_station_handler};
pub use idf_station::{IdfMetadata, IdfStationAvailability, IdfStationResp, IdfUnit, IdfValue};

pub fn set_routes() -> Router<EgressState> {
    Router::new()
        .route("/idf/station", get(idf_station_availability_handler))
        .route("/idf/station/{station_id}", get(idf_station_handler))
        // TODO: add route to query all available stations with PT1M precipitation observations?
        .route("/idf/event", get(idf_event_availability_handler))
        .route("/idf/event/{station_id}", get(idf_event_handler))
}
