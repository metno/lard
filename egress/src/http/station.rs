use axum::{
    extract::{Json, Path, Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::timeseries::{
    Timeseries, get_timeseries_data_irregular, get_timeseries_data_regular, get_timeseries_info,
};
use ::util::{DbPools, http_error::internal};

#[derive(Debug, Deserialize)]
pub struct Params {
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    time_resolution: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Resp {
    pub tseries: Vec<Timeseries>,
}

pub async fn handler(
    State(pools): State<DbPools>,
    // TODO: this should probably take element_id instead of param_id and do a conversion
    Path((station_id, param_id)): Path<(i32, i32)>,
    Query(params): Query<Params>,
) -> Result<Json<Resp>, (StatusCode, String)> {
    let conn = pools.open.get().await.map_err(internal)?;

    let header = get_timeseries_info(&conn, station_id, param_id)
        .await
        .map_err(internal)?;

    let start_time = params.start_time.unwrap_or(header.fromtime);
    let end_time = params.end_time.unwrap_or(header.totime);

    let ts = if let Some(time_resolution) = params.time_resolution {
        Timeseries::Regular(
            get_timeseries_data_regular(&conn, header, start_time, end_time, time_resolution)
                .await
                .map_err(internal)?,
        )
    } else {
        Timeseries::Irregular(
            get_timeseries_data_irregular(&conn, header, start_time, end_time)
                .await
                .map_err(internal)?,
        )
    };

    Ok(Json(Resp { tseries: vec![ts] }))
}
