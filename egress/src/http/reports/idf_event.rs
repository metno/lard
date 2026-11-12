use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    patchwork,
    reports::{
        idf_event::{
            DEFAULT_DURATIONS, DEFAULT_LEVEL, DEFAULT_SENSOR, IdfEvent, MAX_ALLOWED_DURATION,
            PRECIPITATION_PARAM_ID, collect_idf_events, fetch_rain_data, is_idf_event_timeseries,
        },
        idf_station::Unit,
    },
};
use util::{
    DbPools, auth::Auth, deserialize::optional_comma_separated, http_error::internal,
    timerange::ClosedTimerange,
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
    let timerange = ClosedTimerange {
        from: params.fromtime,
        to: params.totime,
    };

    let (open_data, restricted_data) = tokio::try_join!(
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

    // NOTE: given how permits work at the moment, these are mutually exclusive
    let data = match (open_data.is_empty(), restricted_data.is_empty()) {
        (false, _) => open_data,
        (_, false) => restricted_data,
        (true, true) => {
            return Err((
                StatusCode::NOT_FOUND,
                "no data found for this station".to_string(),
            ));
        }
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
pub struct Available {
    pub station_id: i32,
    pub permit: i32,
    pub from: DateTime<Utc>,
    pub to: Option<DateTime<Utc>>,
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

    let mut stations: Vec<_> = ot
        .iter()
        .filter(|(label, _)| is_idf_event_timeseries(label))
        .map(|(label, fills)| Available {
            station_id: label.station_id,
            permit: fills[0].permit,
            from: fills[0].from,
            to: fills.iter().last().unwrap().to,
        })
        .collect();

    if !auth.station_roles.is_empty() || !auth.permit_roles.is_empty() {
        let rt = tables.restricted.read().map_err(internal)?;

        stations.extend(
            rt.iter()
                .filter(|(label, _)| is_idf_event_timeseries(label))
                // NOTE: All fills should have the same permit id since restrictions are applied to whole
                // stations or single params
                .filter(|(label, fills)| {
                    auth.permit_roles.contains(&fills[0].permit)
                        || auth.station_roles.contains(&label.station_id)
                })
                .map(|(label, fills)| Available {
                    station_id: label.station_id,
                    permit: fills[0].permit,
                    from: fills[0].from,
                    to: fills.iter().last().unwrap().to,
                }),
        );
    }

    // TODO: should this be sorted by station_id?
    Ok(Json(AvailabilityResp { stations }))
}
