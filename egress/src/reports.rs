use axum::Router;

use crate::{EgressState, PgConnectionPool};

mod idf_station;
use idf_station::idf_station_router;
pub use idf_station::{IdfStationAvailability, IdfStationResp};

pub fn reports_routes() -> Router<EgressState> {
    Router::new().nest("/idf", idf_station_router())
}
