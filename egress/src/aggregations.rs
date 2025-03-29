use crate::{internal_error, timeseries::get_timeseries_info, PgConnectionPool};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub enum AggregationType {
    Max,
    Min,
    Avg,
}

// TODO: Just use RelativeDuration?
#[derive(Debug, Deserialize)]
pub enum AggregationPeriod {
    Hourly,
    Diurnal,
    Daily,
    Monthly,
    Yearly,
}

#[derive(Debug, Deserialize)]
pub struct AggregationParams {
    agg_type: AggregationType,
    period: AggregationPeriod,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AggregationResp {
    // TODO: Change me
    pub data: Vec<()>,
}

pub async fn aggregation_handler(
    State(pool): State<PgConnectionPool>,
    // TODO: Should param here be something other than param_id? Perhaps some kind of abstract
    // param that represents several specific ones, like temperature instead of TA, TAX, TAM, TAN, etc.
    Path((station_id, param_id)): Path<(i32, i32)>,
    Query(query_params): Query<AggregationParams>,
) -> Result<Json<AggregationResp>, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let headers = get_timeseries_info(&conn, station_id, param_id);
    todo!()
}
