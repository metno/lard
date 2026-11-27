use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    patchwork::{self, Patchwork},
    reports::{
        WINDROSE_AVAILABLE_REQUESTS_RECEIVED, WINDROSE_REQUESTS_RECEIVED,
        windrose::{
            DIRECTION_AXIS, Metadata, SPEED_AXIS, WIND_DIRECTION_PARAM_ID, WindData, Windrose,
            create_default_label, fetch_wind_data, is_wind_speed_timeseries,
        },
    },
};
use util::{
    DbPools, auth::Auth, deserialize::optional_comma_separated, http_error::internal,
    timerange::closed,
};

/// Query parameter for reports/windrose/{station_id} endpoint
#[derive(Debug, Serialize, Deserialize)]
pub struct Params {
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
    #[serde(default, deserialize_with = "optional_comma_separated")]
    months: Option<Vec<i32>>,
}

/// Response from reports/windrose/{station_id} endpoint
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Resp {
    pub metadata: Metadata,
    pub windrose: Windrose,
}

pub async fn handler(
    Path(station_id): Path<i32>,
    Query(params): Query<Params>,
    State(pools): State<DbPools>,
    State(tables): State<patchwork::Tables>,
    auth: Auth,
) -> Result<Json<Resp>, (StatusCode, String)> {
    metrics::counter!(WINDROSE_REQUESTS_RECEIVED).increment(1);
    let timerange = closed(params.fromtime, params.totime);

    // NOTE: given how permits work at the moment, open and restricted are mutually exclusive
    let ((open_sources, open_data), (restricted_sources, restricted_data)) = tokio::try_join!(
        fetch_wind_data(
            station_id,
            timerange,
            &params.months,
            &auth.permit_roles,
            &auth.station_roles,
            pools.open,
            tables.open
        ),
        fetch_wind_data(
            station_id,
            timerange,
            &params.months,
            &auth.permit_roles,
            &auth.station_roles,
            pools.restricted,
            tables.restricted
        ),
    )
    .map_err(internal)?;

    let (speed_sources, direction_sources, WindData { days }) =
        match (open_data.is_empty(), restricted_data.is_empty()) {
            (false, _) => (open_sources.0, open_sources.1, open_data),
            (_, false) => (restricted_sources.0, restricted_sources.1, restricted_data),
            (true, true) => {
                return Err((
                    StatusCode::NOT_FOUND,
                    "no data found for this station".to_string(),
                ));
            }
        };

    let windrose =
        tokio::task::spawn_blocking(|| Windrose::new_from_days(SPEED_AXIS, DIRECTION_AXIS, days))
            .await
            .map_err(internal)?;

    let metadata = Metadata {
        speed_sources,
        direction_sources,
    };

    Ok(Json(Resp { metadata, windrose }))
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Available {
    pub station_id: i32,
    pub metadata: Metadata,
}

impl Available {
    pub fn from_speed_entry(
        (label, speed_patchwork): (&patchwork::Label, &Patchwork),
        table: &patchwork::Table,
    ) -> Option<Self> {
        let direction_patchwork = table.get(&create_default_label(
            label.station_id,
            WIND_DIRECTION_PARAM_ID,
        ))?;

        Some(Available {
            station_id: label.station_id,
            metadata: Metadata {
                speed_sources: speed_patchwork.clone(),
                direction_sources: direction_patchwork.clone(),
            },
        })
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct AvailabilityResp {
    pub stations: Vec<Available>,
}

pub async fn availability_handler(
    State(tables): State<patchwork::Tables>,
    auth: Auth,
) -> Result<Json<AvailabilityResp>, (StatusCode, String)> {
    metrics::counter!(WINDROSE_AVAILABLE_REQUESTS_RECEIVED).increment(1);
    let ot = tables.open.read().map_err(internal)?;
    let open = ot
        .iter()
        .filter(|(label, _)| is_wind_speed_timeseries(label))
        // Check that the filtered stations also have a corresponding wind direction timeseries
        // NOTE: this only works if the timeseries are both open or both restricted,
        // if they are somehow mixed we need to implement something different
        .filter_map(|entry| Available::from_speed_entry(entry, &ot));

    let stations = if !auth.permit_roles.is_empty() || !auth.station_roles.is_empty() {
        let rt = tables.restricted.read().map_err(internal)?;
        let restricted = rt
            .iter()
            .filter(|(label, _)| is_wind_speed_timeseries(label))
            // NOTE: All fills should have the same permit id since restrictions are applied
            // to whole stations or single params
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
            // Check that the filtered stations also have a corresponding wind direction timeseries
            .filter_map(|entry| Available::from_speed_entry(entry, &ot))
            // check that direction patches are also only with allowed permits
            .filter(|available| {
                available.metadata.direction_sources.iter().all(|patch| {
                    patch
                        .source
                        .as_ref()
                        // more strict than it needs to be, assumes all permits are the same.
                        // should reduce strictness or encode assumption in types
                        .is_none_or(|patch| auth.permit_roles.contains(&patch.permit))
                })
            });

        open.chain(restricted).collect()
    } else {
        open.collect()
    };

    Ok(Json(AvailabilityResp { stations }))
}
