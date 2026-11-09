use axum::extract::{Path, Query, State};
use axum::{Json, Router, routing::get};
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
    EgressState,
    calculations::{
        CALCULATIONS_REQUESTS_RECEIVED, CreationParams, apply_calc2, apply_calc3,
        get_calculation_data_pair, get_calculation_data_triple, get_merged_patchset,
        humidity::{
            dew_point_temperature, humidity_mixing_ratio, specific_humidity,
            water_vapor_partial_pressure_in_air,
        },
    },
    patchwork,
};
use ::util::{DbPools, auth::Auth, deserialize::optional_comma_separated, http_error::internal};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Params {
    level: Option<i32>,
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "optional_comma_separated")]
    accepted_qc: Option<Vec<i32>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Resp {
    pub data: Vec<(DateTime<Utc>, f64)>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Available {
    param_id: i32,
    endpoint: String,
}

async fn calculation_pair_handler(
    pools: DbPools,
    patchwork_tables: patchwork::Tables,
    station_id: i32,
    params: Params,
    permit_roles: &[i32],
    station_roles: &[i32],
    param_ids: [i32; 2],
) -> Result<Vec<(DateTime<Utc>, [(f64, Option<i32>); 2])>, (StatusCode, String)> {
    metrics::counter!(CALCULATIONS_REQUESTS_RECEIVED).increment(1);
    let open_conn = pools.open.get().await.map_err(internal)?;
    let restricted_conn = pools.restricted.get().await.map_err(internal)?;

    // if to is not provided, default to now if no timeseries have a to time.
    let to_p = params.to.unwrap_or_else(Utc::now);
    // if quality code filter is not provided, default to accepting all quality codes (including null)
    // -1 maps to null in the query, so this allows us to include null quality codes
    let accepted_qc = params
        .accepted_qc
        .unwrap_or_else(|| vec![-1, 0, 1, 2, 3, 4, 5, 6, 7]);

    let parameters = CreationParams {
        station_id,
        param_ids: param_ids.to_vec(),
        level: params.level,
        sensor: params.sensor,
    };

    let open_patches = get_merged_patchset(
        parameters.clone(),
        params.from,
        to_p,
        permit_roles,
        station_roles,
        patchwork_tables.open,
    )
    .map_err(internal)?;

    // get the data for the station and time
    let open_data =
        get_calculation_data_pair(&open_patches, params.from, to_p, &accepted_qc, &open_conn)
            .await
            .map_err(internal)?;

    // don't need to check the restricted table unless might have access
    // NOTE: We expect timeseries to have 1 permit, so if there is data in the open db then we
    // shouldn't look for data in restricted. There can be cases of this happening, but it is a CM issue.
    if (!permit_roles.is_empty() || !station_roles.is_empty()) && open_data.is_empty() {
        let restricted_patches = get_merged_patchset(
            parameters,
            params.from,
            to_p,
            permit_roles,
            station_roles,
            patchwork_tables.restricted,
        )
        .map_err(internal)?;

        // get the data for the station and time
        let restricted_data = get_calculation_data_pair(
            &restricted_patches,
            params.from,
            to_p,
            &accepted_qc,
            &restricted_conn,
        )
        .await
        .map_err(internal)?;
        // only return if found data
        if !restricted_data.is_empty() {
            return Ok(restricted_data);
        }
    }
    if open_data.is_empty() {
        // No data found in either open or restricted, return 404
        return Err((StatusCode::NOT_FOUND, "No data found".to_string()));
    }

    Ok(open_data)
}

async fn calculation_triple_handler(
    pools: DbPools,
    patchwork_tables: patchwork::Tables,
    station_id: i32,
    params: Params,
    permit_roles: &[i32],
    station_roles: &[i32],
    param_ids: [i32; 3],
) -> Result<Vec<(DateTime<Utc>, [(f64, Option<i32>); 3])>, (StatusCode, String)> {
    metrics::counter!(CALCULATIONS_REQUESTS_RECEIVED).increment(1);
    let open_conn = pools.open.get().await.map_err(internal)?;
    let restricted_conn = pools.restricted.get().await.map_err(internal)?;

    // if to is not provided, default to now if no timeseries have a to time.
    let to_p = params.to.unwrap_or_else(Utc::now);
    // if quality code filter is not provided, default to accepting all quality codes (including null)
    // -1 maps to null in the query, so this allows us to include null quality codes
    let accepted_qc = params
        .accepted_qc
        .unwrap_or_else(|| vec![-1, 0, 1, 2, 3, 4, 5, 6, 7]);

    let parameters = CreationParams {
        station_id,
        param_ids: param_ids.to_vec(),
        level: params.level,
        sensor: params.sensor,
    };

    let open_patches = get_merged_patchset(
        parameters.clone(),
        params.from,
        to_p,
        permit_roles,
        station_roles,
        patchwork_tables.open,
    )
    .map_err(internal)?;

    // get the data for the station and time
    let open_data =
        get_calculation_data_triple(&open_patches, params.from, to_p, &accepted_qc, &open_conn)
            .await
            .map_err(internal)?;

    // don't need to check the restricted table unless might have access
    // NOTE: We expect timeseries to have 1 permit, so if there is data in the open db then we
    // shouldn't look for data in restricted. There can be cases of this happening, but it is a CM issue.
    if (!permit_roles.is_empty() || !station_roles.is_empty()) && open_data.is_empty() {
        let restricted_patches = get_merged_patchset(
            parameters,
            params.from,
            to_p,
            permit_roles,
            station_roles,
            patchwork_tables.restricted,
        )
        .map_err(internal)?;

        // get the data for the station and time
        let restricted_data = get_calculation_data_triple(
            &restricted_patches,
            params.from,
            to_p,
            &accepted_qc,
            &restricted_conn,
        )
        .await
        .map_err(internal)?;
        // only return if found data
        if !restricted_data.is_empty() {
            return Ok(restricted_data);
        }
    }
    if open_data.is_empty() {
        // No data found in either open or restricted, return 404
        return Err((StatusCode::NOT_FOUND, "No data found".to_string()));
    }

    Ok(open_data)
}

// makes paramid 217 (dew point temperature) from 211 (temperature) and 262 (relative humidity)
pub async fn dew_point_temperature_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<patchwork::Tables>,
    Path(station_id): Path<i32>,
    Query(params): Query<Params>,
    auth: Auth,
) -> Result<Json<Resp>, (StatusCode, String)> {
    // get the data (either from open or restricted - depending on if open exists, and if user has access to restricted)
    let data = calculation_pair_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        &auth.permit_roles,
        &auth.station_roles,
        [211, 262],
    )
    .await?;

    // do the calculation and coalesce the QC together
    let result: Vec<(DateTime<Utc>, f64)> = data
        .into_iter()
        .map(|(time, d)| apply_calc2(time, d, dew_point_temperature))
        .collect();

    Ok(Json(Resp { data: result }))
}

// makes paramid 3136 (water vapor partial pressure in air) from 211 (temperature) and 262 (relative humidity)
pub async fn water_vapor_partial_pressure_in_air_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<patchwork::Tables>,
    Path(station_id): Path<i32>,
    Query(params): Query<Params>,
    auth: Auth,
) -> Result<Json<Resp>, (StatusCode, String)> {
    // get the data (either from open or restricted - depending on if open exists, and if user has access to restricted)
    let data = calculation_pair_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        &auth.permit_roles,
        &auth.station_roles,
        [211, 262],
    )
    .await?;

    // do the calculation and coalesce the QC together
    let result: Vec<(DateTime<Utc>, f64)> = data
        .into_iter()
        .map(|(time, d)| apply_calc2(time, d, water_vapor_partial_pressure_in_air))
        .collect();

    Ok(Json(Resp { data: result }))
}

// makes 3123 (specific humidity) from 211 (temperature), 262 (relative humidity), and 173 (pressure)
pub async fn specific_humidity_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<patchwork::Tables>,
    Path(station_id): Path<i32>,
    Query(params): Query<Params>,
    auth: Auth,
) -> Result<Json<Resp>, (StatusCode, String)> {
    // get the data (either from open or restricted - depending on if open exists, and if user has access to restricted)
    let data = calculation_triple_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        &auth.permit_roles,
        &auth.station_roles,
        [211, 262, 173],
    )
    .await?;

    // do the calculation and coalesce the QC together
    let result: Vec<(DateTime<Utc>, f64)> = data
        .into_iter()
        .map(|(time, d)| apply_calc3(time, d, specific_humidity))
        .collect();

    Ok(Json(Resp { data: result }))
}

// makes 3197 (humidity mixing ratio) from 211 (temperature), 262 (relative humidity), and 173 (pressure)
pub async fn humidity_mixing_ratio_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<patchwork::Tables>,
    Path(station_id): Path<i32>,
    Query(params): Query<Params>,
    auth: Auth,
) -> Result<Json<Resp>, (StatusCode, String)> {
    // get the data (either from open or restricted - depending on if open exists, and if user has access to restricted)
    let data = calculation_triple_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        &auth.permit_roles,
        &auth.station_roles,
        [211, 262, 173],
    )
    .await?;

    // do the calculation and coalesce the QC together
    let result: Vec<(DateTime<Utc>, f64)> = data
        .into_iter()
        .map(|(time, d)| apply_calc3(time, d, humidity_mixing_ratio))
        .collect();

    Ok(Json(Resp { data: result }))
}

// this endpoint is just for listing what sub endpoints exist for calculations
pub async fn availability_handler() -> Result<Json<Vec<Available>>, (StatusCode, String)> {
    // NOTE: this list should be kept up to date with the implemented calculations
    let response = [217, 3123, 3197, 3136]
        .iter()
        .map(|p| Available {
            param_id: *p,
            endpoint: format!("calculations/station/{{station_id}}/param/{p}"),
        })
        .collect();
    Ok(Json(response))
}

pub fn router() -> Router<EgressState> {
    Router::new()
        .route("/param", get(availability_handler))
        .route(
            "/station/{station_id}/param/217",
            get(dew_point_temperature_handler),
        )
        .route(
            "/station/{station_id}/param/3123",
            get(specific_humidity_handler),
        )
        .route(
            "/station/{station_id}/param/3197",
            get(humidity_mixing_ratio_handler),
        )
        .route(
            "/station/{station_id}/param/3136",
            get(water_vapor_partial_pressure_in_air_handler),
        )
}
