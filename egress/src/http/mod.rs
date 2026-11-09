//! HTTP handlers and routers

use axum::{Router, middleware, routing::get};

use crate::EgressState;

pub mod calculations;
pub mod latest;
pub mod liveness;
pub mod patchwork;
pub mod reports;
pub mod station;
pub mod timeslice;

pub fn router() -> Router<EgressState> {
    Router::new()
        .route(
            // TODO: move the param into the path, since it is not in alignment with other endpoints
            "/patchwork/station/{station_id}", // all parameters sent as query not in url
            get(patchwork::handler),
        )
        .route_layer(middleware::from_fn(
            patchwork::track_patchwork_request_duration,
        ))
        .route("/patchwork/available", get(patchwork::available_handler))
        .route(
            "/station/{station_id}/param/{param_id}",
            get(station::handler),
        )
        .route(
            "/timeslice/{timestamp}/param/{param_id}",
            get(timeslice::handler),
        )
        .route("/latest", get(latest::handler))
        .route("/liveness", get(liveness::handler))
        .nest("/reports", reports::router())
        .nest("/calculations", calculations::router())
}
