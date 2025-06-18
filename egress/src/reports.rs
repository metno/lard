use axum::{routing::get, Router};

use crate::EgressState;

mod idf_station;
use idf_station::{idf_station_availability_handler, idf_station_handler};
pub use idf_station::{IdfMetadata, IdfStationAvailability, IdfStationResp, IdfUnit, IdfValue};

pub fn reports_router() -> Router<EgressState> {
    Router::new()
        .route("/idf/station", get(idf_station_availability_handler))
        .route("/idf/station/{station_id}", get(idf_station_handler))
}
