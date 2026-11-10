use axum::{
    extract::{Json, MatchedPath, Path, Query, Request, State},
    http::StatusCode,
    middleware::Next,
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    PATCHWORK_AVAILABLE_REQUESTS_RECEIVED, PATCHWORK_REQUESTS_RECEIVED,
    patchwork::{Datum, Label, Metadata, Patchwork, Tables, get_patchwork_data},
    util::{default_level_from_api_param, default_sensor_from_api_param},
};
use ::util::{
    DbPools, auth::Auth, http_error::internal, stinfofacade::level::LevelTable, timerange::closed,
};

#[derive(Debug, Deserialize)]
pub struct Params {
    paramid: i32,
    /// None will be converted to the default for level (which may in fact be None in some cases)
    level: Option<i32>,
    /// None will be converted to the default value (0)
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Resp {
    pub metadata: Metadata,
    pub data: Vec<Datum>,
}

pub async fn handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<Tables>,
    State(level_table): State<LevelTable>,
    Path(station_id): Path<i32>,
    Query(params): Query<Params>,
    auth: Auth,
) -> Result<Json<Resp>, (StatusCode, String)> {
    metrics::counter!(PATCHWORK_REQUESTS_RECEIVED).increment(1);

    let level = default_level_from_api_param(level_table.clone(), params.level, params.paramid)
        .map_err(internal)?;
    let sensor = default_sensor_from_api_param(params.sensor);

    let label: Label = Label {
        station_id,
        param_id: params.paramid,
        level,
        sensor: Some(sensor),
    };

    let open_conn = pools.open.get().await.map_err(internal)?;
    let restricted_conn = pools.restricted.get().await.map_err(internal)?;

    // default to now if no to is provided
    let timerange = closed(params.from, params.to.unwrap_or_else(Utc::now));

    let (open_patches, open_data) = get_patchwork_data(
        &open_conn,
        timerange,
        label,
        patchwork_tables.open.clone(),
        &auth.permit_roles,
        &auth.station_roles,
    )
    .await
    .map_err(internal)?;

    // NOTE: We expect timeseries to have 1 permit, so if there is data in the open db then we
    // shouldn't look for data in restricted. There can be cases of this happening, but it is a CM issue.
    if (!auth.permit_roles.is_empty() || !auth.station_roles.is_empty()) && open_data.is_empty() {
        let (restricted_patches, restricted_data) = get_patchwork_data(
            &restricted_conn,
            timerange,
            label,
            patchwork_tables.restricted.clone(),
            &auth.permit_roles,
            &auth.station_roles,
        )
        .await
        .map_err(internal)?;

        if !restricted_data.is_empty() {
            return Ok(Json(Resp {
                metadata: Metadata {
                    sources: restricted_patches,
                },
                data: restricted_data,
            }));
        }
    }
    if open_data.is_empty() {
        // No data found in either open or restricted, return 404
        return Err((StatusCode::NOT_FOUND, "No data found".to_string()));
    }

    Ok(Json(Resp {
        metadata: Metadata {
            sources: open_patches,
        },
        data: open_data,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Available {
    label: Label,
    metadata: Metadata,
}

impl Available {
    pub fn from_entry((label, patchwork): (&Label, &Patchwork)) -> Self {
        Available {
            label: *label,
            metadata: Metadata {
                sources: patchwork.clone(),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AvailableResp {
    pub available: Vec<Available>,
}

pub async fn available_handler(
    State(tables): State<Tables>,
    auth: Auth,
) -> Result<Json<AvailableResp>, (StatusCode, String)> {
    metrics::counter!(PATCHWORK_AVAILABLE_REQUESTS_RECEIVED).increment(1);

    let open_tables = tables.open.read().map_err(internal)?;
    let open = open_tables.iter().map(Available::from_entry);

    let available = if !auth.permit_roles.is_empty() || !auth.station_roles.is_empty() {
        let restricted_tables = tables.restricted.read().map_err(internal)?;
        let restricted = restricted_tables
            .iter()
            .filter(|(label, patchwork)| {
                auth.station_roles.contains(&label.station_id)
                    || patchwork.iter().all(|patch| {
                        patch
                            .source
                            .as_ref()
                            // more strict than it needs to be, assumes all permits are the same.
                            // should reduce strictness or encode assumption in types
                            .is_none_or(|patch| auth.permit_roles.contains(&patch.permit))
                    })
            })
            .map(Available::from_entry);
        open.chain(restricted).collect()
    } else {
        open.collect()
    };

    Ok(Json(AvailableResp { available }))
}

/// Middleware function that runs around a request, so we can record how long it took
pub async fn track_patchwork_request_duration(req: Request, next: Next) -> impl IntoResponse {
    let start = std::time::Instant::now();
    let (path, query) = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        (
            matched_path.as_str().to_owned(),
            req.uri().query().unwrap_or_default().to_owned(),
        )
    } else {
        (
            req.uri().path().to_owned(),
            req.uri().query().unwrap_or_default().to_owned(),
        )
    };
    let method = req.method().to_string();

    let response = next.run(req).await;

    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [("method", method), ("path", path), ("status", status)];

    if duration > 10.0 {
        tracing::info!(
            "Long patchwork request: {} seconds, query params: {:?}",
            duration,
            query
        );
    }

    metrics::histogram!("patchwork_http_requests_duration_seconds", &labels).record(duration);

    response
}
