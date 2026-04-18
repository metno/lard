use axum::http::StatusCode;

/// Utility function for mapping any error into a `500 Internal Server Error` response.
pub fn internal_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

/// Utility function for mapping any error into a `401 Unauthorized` response.
pub fn unauthorized<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::UNAUTHORIZED, err.to_string())
}

/// Utility function for mapping any error into a `404 Not Found Error` response.
pub fn not_found_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, err.to_string())
}

/// Utility function for mapping any error into a `400 Bad Request Error` response.
pub fn bad_request<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, err.to_string())
}
