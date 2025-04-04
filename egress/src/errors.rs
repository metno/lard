use axum::http::StatusCode;

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
pub fn internal_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
