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
        WINDROSE_AVAILABLE_REQUESTS_RECEIVED, WINDROSE_REQUESTS_RECEIVED,
        windrose::{
            DIRECTION_AXIS, Metadata, SPEED_AXIS, WIND_DIRECTION_PARAM_ID, WindData, Windrose,
            create_default_label, fetch_wind_data, is_wind_speed_timeseries,
        },
    },
};
use util::{DbPools, auth::Auth, deserialize::optional_comma_separated, http_error::internal};

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
pub struct Resp<'a> {
    pub metadata: Metadata,
    pub windrose: Windrose<'a>,
}

pub async fn handler<'a>(
    Path(station_id): Path<i32>,
    Query(params): Query<Params>,
    State(pools): State<DbPools>,
    State(tables): State<patchwork::Tables>,
    auth: Auth,
) -> Result<Json<Resp<'a>>, (StatusCode, String)> {
    metrics::counter!(WINDROSE_REQUESTS_RECEIVED).increment(1);

    // NOTE: given how permits work at the moment, open and restricted are mutually exclusive
    let (open_data, restricted_data) = tokio::try_join!(
        fetch_wind_data(
            station_id,
            params.fromtime,
            params.totime,
            &params.months,
            &auth.permit_roles,
            &auth.station_roles,
            pools.open,
            tables.open
        ),
        fetch_wind_data(
            station_id,
            params.fromtime,
            params.totime,
            &params.months,
            &auth.permit_roles,
            &auth.station_roles,
            pools.restricted,
            tables.restricted
        ),
    )
    .map_err(internal)?;

    let WindData {
        fromtime,
        totime,
        days,
    } = match (open_data.is_empty(), restricted_data.is_empty()) {
        (false, _) => open_data,
        (_, false) => restricted_data,
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
        station_id,
        fromtime,
        totime,
        months: params.months,
    };

    Ok(Json(Resp { metadata, windrose }))
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Available {
    pub station_id: i32,
    pub permit: i32,
    pub from: DateTime<Utc>,
    pub to: Option<DateTime<Utc>>,
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
    let mut stations: Vec<_> = {
        let ot = tables.open.read().map_err(internal)?;

        ot.iter()
            .filter(|(label, _)| is_wind_speed_timeseries(label))
            // Check that the filtered stations also have a corresponding wind direction timeseries
            // NOTE: this only works if the timeseries are both open or both restricted,
            // if they are somehow mixed we need to implement something different
            .filter_map(|(label, speed)| {
                let direction = ot.get(&create_default_label(
                    label.station_id,
                    WIND_DIRECTION_PARAM_ID,
                ))?;

                Some((label, speed, direction))
            })
            .map(|(label, speed, direction)| {
                let speed_to = speed.iter().last().unwrap().to;
                let direction_to = direction.iter().last().unwrap().to;

                let to = speed_to.min(direction_to);
                let from = speed[0].from.max(direction[0].from);

                Available {
                    station_id: label.station_id,
                    permit: speed[0].permit,
                    from,
                    to,
                }
            })
            .collect()
    };

    if !auth.permit_roles.is_empty() || !auth.station_roles.is_empty() {
        let rt = tables.restricted.read().map_err(internal)?;

        stations.extend(
            rt.iter()
                .filter(|(label, _)| is_wind_speed_timeseries(label))
                // NOTE: All fills should have the same permit id since restrictions are applied
                // to whole stations or single params
                .filter(|(label, fills)| {
                    auth.permit_roles.contains(&fills[0].permit)
                        || auth.station_roles.contains(&label.station_id)
                })
                // Check that the filtered stations also have a corresponding wind direction timeseries
                .filter_map(|(label, speed)| {
                    let direction = rt.get(&create_default_label(
                        label.station_id,
                        WIND_DIRECTION_PARAM_ID,
                    ))?;

                    Some((label, speed, direction))
                })
                // Check that both timeseries have the same permit
                .filter(|(_, speed, direction)| speed[0].permit == direction[0].permit)
                .map(|(label, speed, direction)| {
                    let speed_to = speed.iter().last().unwrap().to;
                    let direction_to = direction.iter().last().unwrap().to;

                    let to = speed_to.min(direction_to);
                    let from = speed[0].from.max(direction[0].from);

                    Available {
                        station_id: label.station_id,
                        permit: speed[0].permit,
                        from,
                        to,
                    }
                }),
        );
    }

    Ok(Json(AvailabilityResp { stations }))
}
