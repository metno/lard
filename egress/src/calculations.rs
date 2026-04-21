use axum::extract::{Path, Query, State};
use axum::{Extension, Json, Router, routing::get};
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::EgressState;
use crate::PatchworkTables;
use crate::common::{CalculationPatch, merge_patches};
use crate::error;
use crate::error::Error;
use crate::patchwork;
use crate::patchwork::{Fill, Patch};
use util::{DbPools, ParamId, PatchworkLabel, PooledPgConn};
mod humidity;

use crate::calculations::humidity::{
    dew_point_temperature, humidity_mixing_ratio, specific_humidity,
    water_vapor_partial_pressure_in_air,
};

pub const CALCULATIONS_REQUESTS_RECEIVED: &str = "calculations_requests_received";
pub const CALCULATIONS_AVAILABLE_REQUESTS_RECEIVED: &str =
    "calculations_available_requests_received";

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct CalculationParams {
    level: Option<i32>,
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

// label needed for sorting if the patchwork labels have all
// the input paramids for a calculation
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct PotentialCalculationsLabel {
    pub station_id: i32,
    pub level: Option<i32>,
    pub sensor: Option<i32>,
}

#[derive(Debug)]
pub struct ParamsForCalculation {
    pub param_id: ParamId,
    pub fills: Vec<Fill>,
}

async fn get_calculation_data_pair(
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(DateTime<Utc>, (f64, Option<i32>), (f64, Option<i32>))>, Error> {
    let query = conn
        .prepare(
            r#"SELECT
                param1.obstime, param2.obstime,
                param1.original, param2.original,
                param1.corrected, param2.corrected,
                param1.quality_code, param2.quality_code
            FROM (
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $1
                AND obstime >= $3 AND obstime < $4
            ) param1
            INNER JOIN ( 
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $2 
                AND obstime >= $3 AND obstime < $4
            ) param2
            USING (obstime)"#,
        )
        .await?;

    // if to is not provided, default to now if no timeseries have a to time.
    // TODO: handle finding the max from the patches (but if open ended still want now)
    let to_p = to.unwrap_or_else(Utc::now);

    let mut futures = patches
        .iter()
        .map(|patch| async {
            conn.query(&query, &[&patch.tsids[0], &patch.tsids[1], &from, &to_p])
                .await
        })
        .collect::<FuturesOrdered<_>>();

    let mut data = Vec::new();
    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            if let Some((val1, val2)) = row
                .get::<usize, Option<f64>>(2)
                .or(row.get::<usize, Option<f64>>(4))
                .zip(
                    row.get::<usize, Option<f64>>(3)
                        .or(row.get::<usize, Option<f64>>(5)),
                )
            {
                data.push((row.get(0), (val1, row.get(6)), (val2, row.get(7))));
            }
        }
    }

    Ok(data)
}

async fn get_calculation_data_triple(
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
    conn: &PooledPgConn<'_>,
) -> Result<
    Vec<(
        DateTime<Utc>,
        (f64, Option<i32>),
        (f64, Option<i32>),
        (f64, Option<i32>),
    )>,
    Error,
> {
    let query = conn
        .prepare(
            r#"SELECT
                param1.obstime, param2.obstime, param3.obstime,
                param1.original, param2.original, param3.original,
                param1.corrected, param2.corrected, param3.corrected,
                param1.quality_code, param2.quality_code, param3.quality_code
            FROM (
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $1
                AND obstime >= $4 AND obstime < $5
            ) param1
            INNER JOIN (
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $2
                AND obstime >= $4 AND obstime < $5
            ) param2
            USING (obstime)
            INNER JOIN (
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $3
                AND obstime >= $4 AND obstime < $5
            ) param3
            USING (obstime)"#,
        )
        .await?;

    // if to is not provided, default to now if no timeseries have a to time.
    // TODO: handle finding the max from the patches (but if open ended still want now)
    let to_p = to.unwrap_or_else(Utc::now);

    let mut futures = patches
        .iter()
        .map(|patch| async {
            conn.query(
                &query,
                &[
                    &patch.tsids[0],
                    &patch.tsids[1],
                    &patch.tsids[2],
                    &from,
                    &to_p,
                ],
            )
            .await
        })
        .collect::<FuturesOrdered<_>>();

    let mut data = Vec::new();
    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            let value1 = row
                .get::<usize, Option<f64>>(3)
                .or(row.get::<usize, Option<f64>>(6));
            let value2 = row
                .get::<usize, Option<f64>>(4)
                .or(row.get::<usize, Option<f64>>(7));
            let value3 = row.get::<usize, Option<f64>>(5).or(row.get(8));
            if value1.is_none() || value2.is_none() || value3.is_none() {
                continue; // if don't have a value for one of the params, skip this row
            }
            data.push((
                row.get(0),
                (value1.unwrap(), row.get(9)),
                (value2.unwrap(), row.get(10)),
                (value3.unwrap(), row.get(11)),
            ));
        }
    }

    Ok(data)
}

// Get available patches for a single parameter.
fn get_applicable_patches_for_calculation(
    param_id: i32,
    station_id: i32,
    params: CalculationParams,
    roles_permit: &[i32],
    roles_station: &[i32],
    patchwork_tables: PatchworkTables,
) -> Result<Vec<Patch>, Error> {
    let label = PatchworkLabel {
        station_id,
        param_id,
        level: params.level,
        sensor: params.sensor,
    };
    // make the params to time into now if not provided
    let to = params.to.unwrap_or_else(Utc::now);
    let patches = patchwork::get_applicable_timeseries(
        params.from,
        to,
        label,
        roles_permit,
        roles_station,
        patchwork_tables.open,
    )?;
    // only bother if patches is empty, and have some potential access
    if (!roles_permit.is_empty() || !roles_station.is_empty()) && patches.is_empty() {
        let patches_restricted = patchwork::get_applicable_timeseries(
            params.from,
            to,
            label,
            roles_permit,
            roles_station,
            patchwork_tables.restricted,
        )?;
        return Ok(patches_restricted);
    }
    // or we return the open patches (which might be empty if don't have access or if there are no timeseries for this param)
    Ok(patches)
}

/// Get patches for multiple parameters and merge them.
fn get_applicable_timeseries_for_calculations(
    param_ids: &[i32],
    station_id: i32,
    params: CalculationParams,
    roles_permit: &[i32],
    roles_station: &[i32],
    patchwork_tables: PatchworkTables,
) -> Result<Vec<CalculationPatch>, Error> {
    let patches = param_ids
        .iter()
        .map(|param_id| {
            get_applicable_patches_for_calculation(
                *param_id,
                station_id,
                params,
                roles_permit,
                roles_station,
                patchwork_tables.clone(),
            )
        })
        .collect::<Result<Vec<Vec<Patch>>, Error>>()?;

    // sanity check that we have all the params we need
    if patches.len() == param_ids.len() {
        return Ok(merge_patches(patches));
    }
    // TODO: return an appropriate error
    Ok(vec![])
}

async fn calculation_pair_handler(
    pools: DbPools,
    patchwork_tables: PatchworkTables,
    station_id: i32,
    params: CalculationParams,
    roles: Option<(Vec<i32>, Vec<i32>)>,
    param_ids: [i32; 2],
) -> Result<Vec<(DateTime<Utc>, (f64, Option<i32>), (f64, Option<i32>))>, (StatusCode, String)> {
    metrics::counter!(CALCULATIONS_REQUESTS_RECEIVED).increment(1);

    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;

    let (roles_permit, roles_station) = roles.unwrap_or_default();
    let patches = get_applicable_timeseries_for_calculations(
        &param_ids,
        station_id,
        params,
        &roles_permit,
        &roles_station,
        patchwork_tables,
    )
    .map_err(error::internal_error)?;

    let data = get_calculation_data_pair(&patches, params.from, params.to, &open_conn)
        .await
        .map_err(error::internal_error)?;
    Ok(data)
}

async fn calculation_triple_handler(
    pools: DbPools,
    patchwork_tables: PatchworkTables,
    station_id: i32,
    params: CalculationParams,
    roles: Option<(Vec<i32>, Vec<i32>)>,
    param_ids: [i32; 3],
) -> Result<
    Vec<(
        DateTime<Utc>,
        (f64, Option<i32>),
        (f64, Option<i32>),
        (f64, Option<i32>),
    )>,
    (StatusCode, String),
> {
    metrics::counter!(CALCULATIONS_REQUESTS_RECEIVED).increment(1);

    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;

    let (roles_permit, roles_station) = roles.unwrap_or_default();
    let patches = get_applicable_timeseries_for_calculations(
        &param_ids,
        station_id,
        params,
        &roles_permit,
        &roles_station,
        patchwork_tables,
    )
    .map_err(error::internal_error)?;

    let data = get_calculation_data_triple(&patches, params.from, params.to, &open_conn)
        .await
        .map_err(error::internal_error)?;
    Ok(data)
}

#[allow(clippy::type_complexity)]
pub fn wrapper_get_calculation_and_qc_from_pair(
    data: (DateTime<Utc>, (f64, Option<i32>), (f64, Option<i32>)),
    f: impl Fn(f64, f64) -> f64,
) -> (DateTime<Utc>, f64, Option<i32>) {
    let (time, d1, d2) = data;
    // apply the calculation function to the values of the two params
    let value = f(d1.0, d2.0);
    let quality_code = d1.1.max(d2.1); // find the worst quality code of the two input params
    (time, value, quality_code)
}

#[allow(clippy::type_complexity)]
pub fn wrapper_get_calculation_and_qc_from_triple(
    data: (
        DateTime<Utc>,
        (f64, Option<i32>),
        (f64, Option<i32>),
        (f64, Option<i32>),
    ),
    f: impl Fn(f64, f64, f64) -> f64,
) -> (DateTime<Utc>, f64, Option<i32>) {
    let (time, d1, d2, d3) = data;
    // apply the calculation function to the values of the three params
    let value = f(d1.0, d2.0, d3.0);
    let quality_code = d1.1.max(d2.1).max(d3.1); // find the worst quality code of the three input params
    (time, value, quality_code)
}

pub async fn dew_point_temperature_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<(DateTime<Utc>, f64, Option<i32>)>>, (StatusCode, String)> {
    let data = calculation_pair_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        roles,
        [211, 262],
    )
    .await?;

    let result: Vec<(DateTime<Utc>, f64, Option<i32>)> = data
        .into_iter()
        .map(|d| wrapper_get_calculation_and_qc_from_pair(d, dew_point_temperature))
        .collect();

    Ok(Json(result))
}

pub async fn water_vapor_partial_pressure_in_air_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<(DateTime<Utc>, f64, Option<i32>)>>, (StatusCode, String)> {
    let data = calculation_pair_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        roles,
        [211, 262],
    )
    .await?;

    let result: Vec<(DateTime<Utc>, f64, Option<i32>)> = data
        .into_iter()
        .map(|d| wrapper_get_calculation_and_qc_from_pair(d, water_vapor_partial_pressure_in_air))
        .collect();

    Ok(Json(result))
}

pub async fn specific_humidity_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<(DateTime<Utc>, f64, Option<i32>)>>, (StatusCode, String)> {
    let data = calculation_triple_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        roles,
        [211, 262, 173],
    )
    .await?;

    let result: Vec<(DateTime<Utc>, f64, Option<i32>)> = data
        .into_iter()
        .map(|d| wrapper_get_calculation_and_qc_from_triple(d, specific_humidity))
        .collect();

    Ok(Json(result))
}

pub async fn humidity_mixing_ratio_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<(DateTime<Utc>, f64, Option<i32>)>>, (StatusCode, String)> {
    let data = calculation_triple_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        roles,
        [211, 262, 173],
    )
    .await?;

    let result: Vec<(DateTime<Utc>, f64, Option<i32>)> = data
        .into_iter()
        .map(|d| wrapper_get_calculation_and_qc_from_triple(d, humidity_mixing_ratio))
        .collect();

    Ok(Json(result))
}

// TODO: can one have spaces in the path of the routes?
pub fn calculations_router() -> Router<EgressState> {
    Router::new()
        .route(
            "/station/{station_id}/217",
            get(dew_point_temperature_handler),
        )
        .route("/station/{station_id}/3123", get(specific_humidity_handler))
        .route(
            "/station/{station_id}/3197",
            get(humidity_mixing_ratio_handler),
        )
        .route(
            "/station/{station_id}/3136",
            get(water_vapor_partial_pressure_in_air_handler),
        )
}
