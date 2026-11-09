//! Handler for basic liveness endpoint.
//!
//! The load balancer needs this to decide when to include an instance in the
//! rotation. If it fails to get a success result from a given instance, it
//! will not be included, and no requests will be routed to it.

use axum::http::StatusCode;

pub async fn handler() -> Result<String, (StatusCode, String)> {
    Ok("Liveness check successful".to_string())
}
