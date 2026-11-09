use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::timeslice::{Timeslice, get_timeslice};
use ::util::{DbPools, http_error::internal};

#[derive(Debug, Serialize, Deserialize)]
pub struct Resp {
    pub tslices: Vec<Timeslice>,
}

pub async fn handler(
    State(pools): State<DbPools>,
    // TODO: this should probably take element_id instead of param_id and do a conversion
    Path((timestamp, param_id)): Path<(DateTime<Utc>, i32)>,
) -> Result<Json<Resp>, (StatusCode, String)> {
    let conn = pools.open.get().await.map_err(internal)?;

    let slice = get_timeslice(&conn, timestamp, param_id)
        .await
        .map_err(internal)?;

    Ok(Json(Resp {
        tslices: vec![slice],
    }))
}
