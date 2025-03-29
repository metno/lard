use crate::{
    internal_error,
    timeseries::{get_timeseries_info, TimeseriesInfo},
    PgConnectionPool,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chronoutil::RelativeDuration;
use serde::{Deserialize, Serialize};
use util::type_id_to_time_resolution;

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

impl Into<RelativeDuration> for AggregationPeriod {
    fn into(self) -> RelativeDuration {
        match self {
            Self::Hourly => RelativeDuration::hours(1),
            Self::Diurnal => RelativeDuration::hours(12),
            Self::Daily => RelativeDuration::days(1),
            Self::Monthly => RelativeDuration::months(1),
            Self::Yearly => RelativeDuration::years(1),
        }
    }
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

    let target_timeres: RelativeDuration = query_params.period.into();

    let headers = get_timeseries_info(&conn, station_id, param_id)
        .await
        .map_err(internal_error)?;

    let (source_timeseres, source_header): (RelativeDuration, TimeseriesInfo) = headers
        .into_iter()
        // Keep only timeseries with type_ids that map to resolutions smaller than the target
        .filter_map(|header| {
            type_id_to_time_resolution(header.type_id)
                .filter(|time_res| *time_res < target_timeres)
                .map(|time_resolution| (time_resolution, header))
        })
        // Take the one left with the largest resolution
        // TODO: Is this right? In the case of a station-aggregated ts it probably is, but if it's
        // instantaneous measurements, we probably want the smallest?
        .max_by_key(|header| header.0)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "No matching timeseries found".to_string(),
            )
        })?;

    todo!()
}
