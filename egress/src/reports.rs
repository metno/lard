use axum::{routing::get, Router};

use crate::EgressState;

mod idf_event;
use idf_event::{idf_event_availability_handler, idf_event_handler};
pub use idf_event::{
    IdfEvent, IdfEventAvailabilityResp, IdfEventAvailable, IdfEventResp, DEFAULT_DURATIONS,
};

mod idf_station;
use idf_station::{idf_station_availability_handler, idf_station_handler};
pub use idf_station::{IdfStationAvailability, IdfStationResp, IdfUnit};

mod windrose;
use windrose::{windrose_availability_handler, windrose_handler};
pub use windrose::{WindCategories, WindroseAvailabilityResp, WindroseAvailable, WindroseResp};

pub fn reports_router() -> Router<EgressState> {
    Router::new()
        .route("/idf/station", get(idf_station_availability_handler))
        .route("/idf/station/{station_id}", get(idf_station_handler))
        .route("/idf/event", get(idf_event_availability_handler))
        .route("/idf/event/{station_id}", get(idf_event_handler))
        .route("/windrose/", get(windrose_availability_handler))
        .route("/windrose/{station_id}", get(windrose_handler))
}
