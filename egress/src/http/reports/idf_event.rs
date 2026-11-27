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
        idf_event::{
            DEFAULT_DURATIONS, DEFAULT_LEVEL, DEFAULT_SENSOR, IdfEvent, MAX_ALLOWED_DURATION,
            PRECIPITATION_PARAM_ID, collect_idf_events, fetch_rain_data, is_idf_event_timeseries,
        },
        idf_station::Unit,
    },
};
use ::util::{
    DbPools, PermitId, auth::Auth, deserialize::optional_comma_separated, http_error::internal,
    timerange::closed,
};

/// Query parameters struct for the idf/event/{station_id} endpoint
#[derive(Serialize, Deserialize)]
pub struct Params {
    #[serde(default)]
    unit: Unit,
    #[serde(default, deserialize_with = "optional_comma_separated")]
    durations: Option<Vec<u32>>,
    fromtime: DateTime<Utc>,
    totime: DateTime<Utc>,
}

/// Response struct returned by the idf/event/{station_id} endpoint
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resp {
    pub station_id: i32,
    pub unit: Unit,
    pub values: Vec<IdfEvent>,
}

pub async fn handler(
    Path(station_id): Path<i32>,
    State(pools): State<DbPools>,
    State(tables): State<patchwork::Tables>,
    Query(params): Query<Params>,
    auth: Auth,
) -> Result<Json<Resp>, (StatusCode, String)> {
    let idf_label = patchwork::Label::new(
        station_id,
        PRECIPITATION_PARAM_ID,
        DEFAULT_LEVEL,
        DEFAULT_SENSOR,
    );
    let timerange = closed(params.fromtime, params.totime);

    let (open, restricted) = tokio::try_join!(
        fetch_rain_data(
            idf_label,
            timerange,
            &auth.permit_roles,
            &auth.station_roles,
            pools.open,
            tables.open
        ),
        fetch_rain_data(
            idf_label,
            timerange,
            &auth.permit_roles,
            &auth.station_roles,
            pools.restricted,
            tables.restricted
        ),
    )
    .map_err(internal)?;

    let Some((_, data)) = open.or(restricted) else {
        return Err((
            StatusCode::NOT_FOUND,
            "no data found for this station".to_string(),
        ));
    };

    // We allow any provided duration that is less or equal to `MAX_ALLOWED_DURATION`
    let inputs: Option<Vec<u32>> = params.durations.map(|values| {
        values
            .into_iter()
            .filter(|val| *val <= MAX_ALLOWED_DURATION)
            .collect()
    });

    let durations = inputs.as_deref().unwrap_or(DEFAULT_DURATIONS);
    let values = collect_idf_events(durations, data, params.unit);

    Ok(Json(Resp {
        station_id,
        unit: params.unit,
        values,
    }))
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    pub sources: Patchwork,
    pub permit: PermitId,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Available {
    pub station_id: i32,
    pub metadata: Metadata,
}

impl Available {
    pub fn from_entry(
        (label, (permit, patchwork)): (&patchwork::Label, &(PermitId, Patchwork)),
    ) -> Self {
        Available {
            station_id: label.station_id,
            metadata: Metadata {
                sources: patchwork.clone(),
                permit: *permit,
            },
        }
    }
}

/// Response struct returned by the availability endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct AvailabilityResp {
    /// List of stations for which IDF calculation is possible
    pub stations: Vec<Available>,
}

pub async fn availability_handler(
    State(tables): State<patchwork::Tables>,
    auth: Auth,
) -> Result<Json<AvailabilityResp>, (StatusCode, String)> {
    let ot = tables.open.read().map_err(internal)?;

    let open = ot
        .iter()
        .filter(|(label, _)| is_idf_event_timeseries(label))
        .map(Available::from_entry);

    let stations = if !auth.station_roles.is_empty() || !auth.permit_roles.is_empty() {
        let rt = tables.restricted.read().map_err(internal)?;
        let restricted = rt
            .iter()
            .filter(|(label, _)| is_idf_event_timeseries(label))
            .filter(|(label, (permit, _))| {
                auth.station_roles.contains(&label.station_id) || auth.permit_roles.contains(permit)
            })
            .map(Available::from_entry);

        open.chain(restricted).collect()
    } else {
        open.collect()
    };

    // TODO: should this be sorted by station_id?
    Ok(Json(AvailabilityResp { stations }))
}
