use axum::{Router, routing::get};

use crate::EgressState;

mod idf_event;
pub use idf_event::{
    DEFAULT_DURATIONS, IdfEvent, IdfEventAvailabilityResp, IdfEventAvailable, IdfEventResp,
};
use idf_event::{idf_event_availability_handler, idf_event_handler};

mod idf_station;
pub use idf_station::{IdfStationAvailability, IdfStationResp, IdfUnit};
use idf_station::{idf_station_availability_handler, idf_station_handler};

mod windrose;
pub use windrose::{WindCategories, WindroseAvailabilityResp, WindroseAvailable, WindroseResp};
use windrose::{windrose_availability_handler, windrose_handler};

mod dut;
pub use dut::{DutAvailability, DutResp, DutUnit};
use dut::{dut_availability_handler, dut_handler};

mod normals;
pub use normals::{NormalsAvailability, NormalsResp};
use normals::{normals_availability_handler, normals_handler};

pub const WINDROSE_REQUESTS_RECEIVED: &str = "windrose_requests_received";
pub const WINDROSE_AVAILABLE_REQUESTS_RECEIVED: &str = "windrose_available_requests_received";

pub fn reports_router() -> Router<EgressState> {
    Router::new()
        .route("/dut/{municipality_id}", get(dut_handler))
        .route("/dut", get(dut_availability_handler))
        .route("/idf/station", get(idf_station_availability_handler))
        .route("/idf/station/{station_id}", get(idf_station_handler))
        .route("/idf/event", get(idf_event_availability_handler))
        .route("/idf/event/{station_id}", get(idf_event_handler))
        .route("/windrose", get(windrose_availability_handler))
        .route("/windrose/{station_id}", get(windrose_handler))
        .route("/normals", get(normals_availability_handler))
        .route("/normals/{station_id}", get(normals_handler))
}
