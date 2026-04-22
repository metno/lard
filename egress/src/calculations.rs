use axum::extract::{Path, Query, State};
use axum::{Extension, Json, Router, routing::get};
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

use crate::{
    EgressState, PatchworkTables,
    common::{CalculationPatch, merge_patches},
    error::{self, Error},
    patchwork::PatchworkTimeseriesTable,
    patchwork::{Patch, get_applicable_timeseries},
};
use util::{DbPools, PatchworkLabel, PooledPgConn};

mod humidity;
use humidity::{
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CalculationLabel {
    station_id: i32,
    param_ids: Vec<i32>, // for calculations, we need to specify multiple params
    level: Option<i32>,
    sensor: Option<i32>,
}

async fn get_calculation_data_pair(
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(DateTime<Utc>, [(f64, Option<i32>); 2])>, Error> {
    let query = conn
        .prepare(
            r#"SELECT
                obstime,
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

    let mut futures = patches
        .iter()
        .map(|patch| async {
            conn.query(&query, &[&patch.tsids[0], &patch.tsids[1], &from, &to])
                .await
        })
        .collect::<FuturesOrdered<_>>();

    let mut data = Vec::new();
    while let Some(res) = futures.next().await {
        let rows = res?;

        for row in rows {
            if let Some((val1, val2)) = row
                .get::<usize, Option<f64>>(1)
                .or(row.get::<usize, Option<f64>>(3))
                .zip(
                    row.get::<usize, Option<f64>>(2)
                        .or(row.get::<usize, Option<f64>>(4)),
                )
            {
                data.push((row.get(0), [(val1, row.get(5)), (val2, row.get(6))]));
            }
        }
    }

    Ok(data)
}

async fn get_calculation_data_triple(
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(DateTime<Utc>, [(f64, Option<i32>); 3])>, Error> {
    let query = conn
        .prepare(
            r#"SELECT
                obstime,
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
                    &to,
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
                .get::<usize, Option<f64>>(1)
                .or(row.get::<usize, Option<f64>>(4));
            let value2 = row
                .get::<usize, Option<f64>>(2)
                .or(row.get::<usize, Option<f64>>(5));
            let value3 = row
                .get::<usize, Option<f64>>(3)
                .or(row.get::<usize, Option<f64>>(6));
            if value1.is_none() || value2.is_none() || value3.is_none() {
                continue; // if don't have a value for one of the params, skip this row
            }
            data.push((
                row.get(0),
                [
                    (value1.unwrap(), row.get(7)),
                    (value2.unwrap(), row.get(8)),
                    (value3.unwrap(), row.get(9)),
                ],
            ));
        }
    }

    Ok(data)
}

// Get available patches for a single parameter.
fn get_patchset(
    label: PatchworkLabel,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    roles_permit: &[i32],
    roles_station: &[i32],
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<Patch>, Error> {
    // this function handles the checking auth part...
    let patches = get_applicable_timeseries(
        from,
        to,
        label,
        roles_permit,
        roles_station,
        patchwork_table,
    )?;

    Ok(patches)
}

/// Get patches for multiple parameters and merge them.
fn get_merged_patchset(
    label: CalculationLabel,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    roles_permit: &[i32],
    roles_station: &[i32],
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<CalculationPatch>, Error> {
    let patches = label
        .param_ids
        .iter()
        .map(|param_id| {
            let patchwork_label = PatchworkLabel {
                station_id: label.station_id,
                param_id: *param_id,
                level: label.level,
                sensor: label.sensor,
            };
            get_patchset(
                patchwork_label,
                from,
                to,
                roles_permit,
                roles_station,
                patchwork_table.clone(),
            )
        })
        .collect::<Result<Vec<Vec<Patch>>, Error>>()?;

    // sanity check that we have all the params we need
    if patches.len() == label.param_ids.len() {
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
) -> Result<Vec<(DateTime<Utc>, [(f64, Option<i32>); 2])>, (StatusCode, String)> {
    metrics::counter!(CALCULATIONS_REQUESTS_RECEIVED).increment(1);

    // if to is not provided, default to now if no timeseries have a to time.
    let to_p = params.to.unwrap_or_else(Utc::now);
    let label = CalculationLabel {
        station_id,
        param_ids: param_ids.to_vec(),
        level: params.level,
        sensor: params.sensor,
    };

    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;

    let (roles_permit, roles_station) = roles.unwrap_or_default();
    let patches = get_merged_patchset(
        label.clone(),
        params.from,
        to_p,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;
    let patches_restricted = get_merged_patchset(
        label,
        params.from,
        to_p,
        &roles_permit,
        &roles_station,
        patchwork_tables.restricted.clone(),
    )
    .map_err(error::internal_error)?;

    let mut data = get_calculation_data_pair(&patches, params.from, to_p, &open_conn)
        .await
        .map_err(error::internal_error)?;

    // see if need to deal with restricted...
    if !patches_restricted.is_empty() {
        let restricted_conn = pools
            .restricted
            .get()
            .await
            .map_err(error::internal_error)?;
        let restricted_data =
            get_calculation_data_pair(&patches_restricted, params.from, to_p, &restricted_conn)
                .await
                .map_err(error::internal_error)?;
        data.extend(restricted_data);
    }

    Ok(data)
}

async fn calculation_triple_handler(
    pools: DbPools,
    patchwork_tables: PatchworkTables,
    station_id: i32,
    params: CalculationParams,
    roles: Option<(Vec<i32>, Vec<i32>)>,
    param_ids: [i32; 3],
) -> Result<Vec<(DateTime<Utc>, [(f64, Option<i32>); 3])>, (StatusCode, String)> {
    metrics::counter!(CALCULATIONS_REQUESTS_RECEIVED).increment(1);

    // if to is not provided, default to now if no timeseries have a to time.
    let to_p = params.to.unwrap_or_else(Utc::now);
    let label = CalculationLabel {
        station_id,
        param_ids: param_ids.to_vec(),
        level: params.level,
        sensor: params.sensor,
    };

    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;

    let (roles_permit, roles_station) = roles.unwrap_or_default();
    let patches = get_merged_patchset(
        label.clone(),
        params.from,
        to_p,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;
    let patches_restricted = get_merged_patchset(
        label,
        params.from,
        to_p,
        &roles_permit,
        &roles_station,
        patchwork_tables.restricted.clone(),
    )
    .map_err(error::internal_error)?;

    let mut data = get_calculation_data_triple(&patches, params.from, to_p, &open_conn)
        .await
        .map_err(error::internal_error)?;

    // see if need to deal with restricted...
    if !patches_restricted.is_empty() {
        let restricted_conn = pools
            .restricted
            .get()
            .await
            .map_err(error::internal_error)?;
        let restricted_data =
            get_calculation_data_triple(&patches_restricted, params.from, to_p, &restricted_conn)
                .await
                .map_err(error::internal_error)?;
        data.extend(restricted_data);
    }

    Ok(data)
}

fn coalesce_qc(qc1: Option<i32>, qc2: Option<i32>) -> Option<i32> {
    match (qc1, qc2) {
        // TODO: need to define how to combine two QC values (max is not good enough).
        (Some(q1), Some(q2)) => Some(q1.max(q2)), // if both have a QC, take the 'worst' one
        (Some(q), None) | (None, Some(q)) => Some(q), // if only one has a QC, take that one
        (None, None) => None,                     // if neither has a QC, return None
    }
}

/// wrapper to apply a calculation to some data, while coalescing the QC of the data together
fn apply_calc2(
    timestamp: DateTime<Utc>,
    data: [(f64, Option<i32>); 2],
    f: impl Fn(f64, f64) -> f64,
) -> (DateTime<Utc>, f64, Option<i32>) {
    // apply the calculation function to the values of the two params
    // choosing the corrected value if it exists, otherwise the original value
    let value = match (data[0].1.is_some(), data[1].1.is_some()) {
        (true, true) => f(data[0].1.unwrap() as f64, data[1].1.unwrap() as f64),
        (true, false) => f(data[0].1.unwrap() as f64, data[1].0),
        (false, true) => f(data[0].0, data[1].1.unwrap() as f64),
        (false, false) => f(data[0].0, data[1].0),
    };
    let quality_code = coalesce_qc(data[0].1, data[1].1);
    (timestamp, value, quality_code)
}

/// see `[apply_calc2]` but for calculations that take 3 inputs
fn apply_calc3(
    timestamp: DateTime<Utc>,
    data: [(f64, Option<i32>); 3],
    f: impl Fn(f64, f64, f64) -> f64,
) -> (DateTime<Utc>, f64, Option<i32>) {
    // apply the calculation function to the values of the three params
    // choosing the corrected value if it exists, otherwise the original value
    let value = match (
        data[0].1.is_some(),
        data[1].1.is_some(),
        data[2].1.is_some(),
    ) {
        (true, true, true) => f(
            data[0].1.unwrap() as f64,
            data[1].1.unwrap() as f64,
            data[2].1.unwrap() as f64,
        ),
        (true, true, false) => f(
            data[0].1.unwrap() as f64,
            data[1].1.unwrap() as f64,
            data[2].0,
        ),
        (true, false, true) => f(
            data[0].1.unwrap() as f64,
            data[1].0,
            data[2].1.unwrap() as f64,
        ),
        (true, false, false) => f(data[0].1.unwrap() as f64, data[1].0, data[2].0),
        (false, true, true) => f(
            data[0].0,
            data[1].1.unwrap() as f64,
            data[2].1.unwrap() as f64,
        ),
        (false, true, false) => f(data[0].0, data[1].1.unwrap() as f64, data[2].0),
        (false, false, true) => f(data[0].0, data[1].0, data[2].1.unwrap() as f64),
        (false, false, false) => f(data[0].0, data[1].0, data[2].0),
    };
    let quality_code = coalesce_qc(coalesce_qc(data[0].1, data[1].1), data[2].1);
    (timestamp, value, quality_code)
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
        .map(|(time, d)| apply_calc2(time, d, dew_point_temperature))
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
        .map(|(time, d)| apply_calc2(time, d, water_vapor_partial_pressure_in_air))
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
        .map(|(time, d)| apply_calc3(time, d, specific_humidity))
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
        .map(|(time, d)| apply_calc3(time, d, humidity_mixing_ratio))
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
