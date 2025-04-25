use axum::Router;

use crate::PgConnectionPool;

mod idf_station;
use idf_station::idf_station_router;
pub use idf_station::{IdfStationAvailability, IdfStationResp};

pub fn reports_routes() -> Router<PgConnectionPool> {
    Router::new().nest("/idf", idf_station_router())
}
