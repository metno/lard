use axum::{Router, routing::get};

use crate::EgressState;

pub mod dut;
pub mod idf_event;
pub mod idf_station;
pub mod normals;
pub mod windrose;

pub fn router() -> Router<EgressState> {
    Router::new()
        .route("/dut/{municipality_id}", get(dut::handler))
        .route("/dut", get(dut::availability_handler))
        .route("/idf/station", get(idf_station::availability_handler))
        .route("/idf/station/{station_id}", get(idf_station::handler))
        .route("/idf/event", get(idf_event::availability_handler))
        .route("/idf/event/{station_id}", get(idf_event::handler))
        .route("/windrose", get(windrose::availability_handler))
        .route("/windrose/{station_id}", get(windrose::handler))
        .route("/normals", get(normals::availability_handler))
        .route("/normals/{station_id}", get(normals::handler))
}
