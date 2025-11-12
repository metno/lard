use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::IntoResponse,
};

/// Middleware function that runs around a request, so we can record how long it took
pub async fn track_request_duration(req: Request, next: Next) -> impl IntoResponse {
    let start = std::time::Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };
    let method = req.method().to_string();

    let response = next.run(req).await;

    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [("method", method), ("path", path), ("status", status)];

    metrics::histogram!("http_requests_duration_seconds", &labels).record(duration);

    response
}
