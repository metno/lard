use axum::extract::{Path, Query, State};
use axum::{Json, Router, routing::get};
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use util::timerange::ClosedTimerange;

use crate::util::default_level_from_api_param;
use crate::{
    EgressState,
    aggregations::{
        Aggregation, AggregationPeriod, AggregationSpec, AggregationType, Metadata, get_aggregation,
    },
    patchwork,
};
use ::util::{
    DbPools, auth::Auth, deserialize::optional_comma_separated, http_error::internal,
    stinfofacade::level::LevelTable,
};

pub const AGGREGATIONS_REQUESTS_RECEIVED: &str = "aggregations_requests_received";

#[derive(Debug, Deserialize, Clone)]
pub struct Params {
    agg_type: AggregationType,
    period: AggregationPeriod,
    // Need a param that adds an offset so that people can create aggregations from 23-23 for example
    offset_hours: Option<i64>,
    level: Option<i32>,
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>, // default to now if not provided
    #[serde(default, deserialize_with = "optional_comma_separated")]
    accepted_qc: Option<Vec<i32>>,
    count_cutoff: Option<bool>, // make it so can turn off filtering for minimum counts (default to true)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Resp {
    pub metadata: Metadata,
    pub data: Vec<Aggregation>,
}

async fn aggregation_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<patchwork::Tables>,
    State(level_table): State<LevelTable>,
    Path((station_id, param_id)): Path<(i32, i32)>,
    Query(params): Query<Params>,
    auth: Auth,
) -> Result<Json<Resp>, (StatusCode, String)> {
    metrics::counter!(AGGREGATIONS_REQUESTS_RECEIVED).increment(1);

    let open_conn = pools.open.get().await.map_err(internal)?;
    let restricted_conn = pools.restricted.get().await.map_err(internal)?;

    let level = default_level_from_api_param(level_table.clone(), params.level, param_id)
        .map_err(internal)?;
    let label = patchwork::Label {
        station_id,
        param_id,
        level,
        sensor: params.sensor.or(Some(0)), // default to sensor 0 if not provided
    };

    // filter the quality code of the underlying data (like in calculations)
    // TODO: should this be the default list?
    let accepted_qc = params
        .accepted_qc
        .unwrap_or_else(|| vec![-1, 0, 1, 2, 3, 4, 5, 6, 7]);

    // should filter for minimum counts?
    let count_cutoff = params.count_cutoff.unwrap_or(true);

    let to = params.to.unwrap_or_else(Utc::now);
    let timerange = ClosedTimerange {
        from: params.from,
        to,
    };

    let offset_hours = params.offset_hours.unwrap_or(0);
    if !(0..=23).contains(&offset_hours) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid offset, must be between 0 and 23 hours".to_string(),
        ));
    }

    let aggregation_spec = AggregationSpec {
        label,
        period: params.period,
        agg_type: params.agg_type,
        offset_hours,
        accepted_qc: accepted_qc.clone(),
        count_cutoff,
    };

    let (open_data, open_sources) = get_aggregation(
        &open_conn,
        timerange,
        aggregation_spec.clone(),
        patchwork_tables.open.clone(),
        &auth.permit_roles,
        &auth.station_roles,
    )
    .await
    .map_err(internal)?;

    let aggregation_period = params.period.as_str();

    // if no data found in open, try restricted if have roles
    if (!auth.permit_roles.is_empty() || !auth.station_roles.is_empty()) && open_data.is_empty() {
        let (restricted_data, restricted_sources) = get_aggregation(
            &restricted_conn,
            timerange,
            aggregation_spec,
            patchwork_tables.restricted.clone(),
            &auth.permit_roles,
            &auth.station_roles,
        )
        .await
        .map_err(internal)?;
        if restricted_data.is_empty() {
            // No data found in either open or restricted, return 404
            return Err((StatusCode::NOT_FOUND, "No data found".to_string()));
        }
        return Ok(Json(Resp {
            metadata: Metadata {
                aggregation_period: aggregation_period.to_string(),
                sources: restricted_sources,
            },
            data: restricted_data,
        }));
    }
    if open_data.is_empty() {
        // No data found return 404
        return Err((StatusCode::NOT_FOUND, "No data found".to_string()));
    }
    Ok(Json(Resp {
        metadata: Metadata {
            aggregation_period: aggregation_period.to_string(),
            sources: open_sources,
        },
        data: open_data,
    }))
}

pub fn router() -> Router<EgressState> {
    Router::new().route(
        "/station/{station_id}/param/{param_id}",
        get(aggregation_handler),
    )
}
