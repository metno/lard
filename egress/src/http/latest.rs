use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::latest::{LatestElem, get_latest};
use ::util::{DbPools, http_error::internal};

#[derive(Debug, Deserialize)]
pub struct Params {
    latest_max_age: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Resp {
    pub data: Vec<LatestElem>,
}

pub async fn handler(
    State(pools): State<DbPools>,
    Query(params): Query<Params>,
) -> Result<Json<Resp>, (StatusCode, String)> {
    let conn = pools.open.get().await.map_err(internal)?;

    let latest_max_age = params
        .latest_max_age
        .unwrap_or_else(|| Utc::now() - Duration::hours(3));

    let data = get_latest(&conn, latest_max_age).await.map_err(internal)?;

    Ok(Json(Resp { data }))
}
