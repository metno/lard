use axum::extract::{Path, Query, State};
use axum::{routing::get, Extension, Json, Router};
use chrono::{DateTime, Utc};
use futures::{stream::FuturesOrdered, StreamExt};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error;
use crate::error::Error;
use crate::patchwork;
use crate::patchwork::{Fill, Patch};
use crate::EgressState;
use crate::PatchworkTables;
use util::{ClosedTimerange, DbPools, OpenTimerange, PatchworkLabel, PooledPgConn};
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
    to: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AvailableParam {
    level: Option<i32>,
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CalculationsAvailableResponse {
    station_id: i32,
    param_id: i32,
    params: Vec<AvailableParam>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataQCtuple {
    value: f64,
    quality_code: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CalculationsResponse {
    timestamp: DateTime<Utc>,
    value: f64,
    underlying_data: Option<HashMap<i32, DataQCtuple>>, // paramid -> (value, quality_code)
}

#[derive(Debug, Clone)]
pub struct CalculationsConstructor {
    label: PotentialCalculationsLabel,
    input_paramids: Vec<(i32, Vec<Fill>)>,
}

// label needed for sorting if the patchwork labels have all
// the input paramids for a calculation
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct PotentialCalculationsLabel {
    pub station_id: i32,
    pub level: Option<i32>,
    pub sensor: Option<i32>,
}

#[derive(Clone, Default, PartialEq, Debug)]
struct CalculationPatch {
    tsids: Vec<i64>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

fn merge_patches(patches_to_merge: Vec<Vec<Patch>>) -> Vec<CalculationPatch> {
    // add the first patch vec as a starting point for the merge
    let mut patches = if let Some(ptm0) = patches_to_merge.first() {
        ptm0.iter()
            .map(|patch| CalculationPatch {
                tsids: vec![patch.tsid],
                from: patch.from,
                to: patch.to,
            })
            .collect()
    } else {
        vec![]
    };

    for ptm in patches_to_merge.into_iter().skip(1) {
        // create a temporary vector to hold the merged patches for this iteration,
        // which will become the new patches vector at the end of the iteration
        let mut new_patches: Vec<CalculationPatch> = Vec::new();
        for p in patches.into_iter() {
            let p_time = ClosedTimerange {
                from: p.from,
                to: p.to,
            };
            for np in ptm.iter() {
                let np_time = ClosedTimerange {
                    from: np.from,
                    to: np.to,
                };
                if let Some(overlap) = p_time.overlap(np_time) {
                    // if there is an overlap, add to the new patches vector with the merged time range and combined tsids
                    let mut np_tsids = vec![np.tsid];
                    np_tsids.extend(p.tsids.iter());
                    let new_p = CalculationPatch {
                        tsids: np_tsids,
                        from: overlap.from,
                        to: overlap.to,
                    };
                    new_patches.push(new_p);
                }
            }
        }
        patches = new_patches; // update the patches vector for the next iteration
    }

    patches // return the merged patches
}

pub fn available_calculations_for_param(
    param_id: i32,
    roles: Option<(Vec<i32>, Vec<i32>)>,
    patchwork_tables: PatchworkTables,
) -> Result<Vec<CalculationsConstructor>, Error> {
    match param_id {
        // "dew_point_temperature"
        217 => get_param_calculations(&[211, 262], roles, patchwork_tables),
        // "specific_humidity"
        3123 => get_param_calculations(&[211, 262, 173], roles, patchwork_tables),
        // "over_time(humidity_mixing_ratio P1D)"
        3197 => get_param_calculations(&[211, 262, 173], roles, patchwork_tables),
        // "mean(water_vapor_partial_pressure_in_air P1D)"
        3136 => get_param_calculations(&[211, 262], roles, patchwork_tables),
        _ => Err(Error::InvalidParam(param_id.to_string())),
    }
}

async fn get_calculation_data_pair<T, Out>(
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    conn: &PooledPgConn<'_>,
    transform: T,
) -> Result<Vec<Out>, Error>
where
    T: Fn(DateTime<Utc>, DataQCtuple, DataQCtuple) -> Option<Out>,
{
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
                .get::<usize, Option<f64>>(2)
                .or(row.get::<usize, Option<f64>>(4))
                .zip(
                    row.get::<usize, Option<f64>>(3)
                        .or(row.get::<usize, Option<f64>>(5)),
                )
            {
                let d1 = DataQCtuple {
                    value: val1,
                    quality_code: row.get(6),
                };
                let d2 = DataQCtuple {
                    value: val2,
                    quality_code: row.get(7),
                };
                if let Some(d) = transform(row.get(0), d1, d2) {
                    data.push(d);
                }
            }
        }
    }

    Ok(data)
}

async fn get_calculation_data_triple<T, Out>(
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    conn: &PooledPgConn<'_>,
    transform: T,
) -> Result<Vec<Out>, Error>
where
    T: Fn(DateTime<Utc>, DataQCtuple, DataQCtuple, DataQCtuple) -> Option<Out>,
{
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
                .get::<usize, Option<f64>>(3)
                .or(row.get::<usize, Option<f64>>(6));
            let value2 = row
                .get::<usize, Option<f64>>(4)
                .or(row.get::<usize, Option<f64>>(7));
            let value3 = row.get::<usize, Option<f64>>(5).or(row.get(8));
            if value1.is_none() || value2.is_none() || value3.is_none() {
                continue; // if don't have a value for one of the params, skip this row
            }
            let d1 = DataQCtuple {
                value: value1.unwrap(),
                quality_code: row.get(9),
            };
            let d2 = DataQCtuple {
                value: value2.unwrap(),
                quality_code: row.get(10),
            };
            let d3 = DataQCtuple {
                value: value2.unwrap(),
                quality_code: row.get(11),
            };
            if let Some(d) = transform(row.get(0), d1, d2, d3) {
                data.push(d);
            }
        }
    }

    Ok(data)
}

fn get_param_calculations(
    input_paramids: &[i32],
    roles: Option<(Vec<i32>, Vec<i32>)>,
    patchwork_tables: PatchworkTables,
) -> Result<Vec<CalculationsConstructor>, Error> {
    let mut found_params: HashMap<PotentialCalculationsLabel, Vec<(i32, Vec<Fill>)>> =
        HashMap::new();

    let ot = patchwork_tables.open.read()?;

    // do not accept data from outside Norway
    let accept_station_id = |key: &PatchworkLabel| key.station_id < 100000;

    for (key, value) in ot.iter().filter(|(k, _)| accept_station_id(k)) {
        // for each calculation, keep anything that could be an input param
        if input_paramids.contains(&key.param_id) {
            let label = PotentialCalculationsLabel {
                station_id: key.station_id,
                level: key.level,
                sensor: key.sensor,
            };
            found_params
                .entry(label)
                .or_default()
                .push((key.param_id, value.to_vec()));
        }
    }
    drop(ot); // release the read lock

    if let Some((roles_permit, roles_station)) = roles {
        let rt = patchwork_tables.open.read()?;
        for (key, value) in rt.iter().filter(|(k, _)| accept_station_id(k)) {
            // for each calculation, keep anything that could be an input param
            if input_paramids.contains(&key.param_id) && roles_station.contains(&key.station_id) {
                let fills_with_allowed_permits: Vec<Fill> = value
                    .iter()
                    .filter(|fill| roles_permit.contains(&fill.permit))
                    .cloned()
                    .collect();
                if !fills_with_allowed_permits.is_empty() {
                    let label = PotentialCalculationsLabel {
                        station_id: key.station_id,
                        level: key.level,
                        sensor: key.sensor,
                    };
                    found_params
                        .entry(label)
                        .or_default()
                        .push((key.param_id, fills_with_allowed_permits));
                }
            }
        }
        drop(rt); // release the read lock
    }
    // if have all the input params for the calculation, then add to available calculations
    // TODO: check the time range... cut down to overlap!
    let param_available = found_params
        .into_iter()
        // keep only those that actually have all the input parameters
        .filter(|(_, value)| value.len() == input_paramids.len())
        // add to the calculation table
        .map(|(key, value)| CalculationsConstructor {
            label: key,
            input_paramids: value,
        })
        .collect();
    Ok(param_available)
}

// Get available patches for a single parameter.
fn get_applicable_timeseries_for_calculation(
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
    let mut patches = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label,
        roles_permit,
        roles_station,
        patchwork_tables.open,
    )?;
    let mut patches_restricted = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label,
        roles_permit,
        roles_station,
        patchwork_tables.restricted,
    )?;
    // put the two vector together TODO: does this make sense?
    patches.append(&mut patches_restricted);
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
            get_applicable_timeseries_for_calculation(
                *param_id,
                station_id,
                params,
                roles_permit,
                roles_station,
                patchwork_tables.clone(),
            )
        })
        .collect::<Result<Vec<Vec<Patch>>, Error>>()?;
    Ok(merge_patches(patches))
}

pub async fn calculations_available_handler(
    Path(param_id): Path<i32>,
    State(patchwork_tables): State<PatchworkTables>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>, // TODO: use the roles for the closed table
) -> Result<Json<Vec<CalculationsAvailableResponse>>, (StatusCode, String)> {
    metrics::counter!(CALCULATIONS_AVAILABLE_REQUESTS_RECEIVED).increment(1);

    let available: Vec<CalculationsConstructor> =
        available_calculations_for_param(param_id, roles, patchwork_tables.clone())
            .map_err(error::internal_error)?;
    let mut available_calculations: HashMap<(i32, i32), Vec<AvailableParam>> = HashMap::new();
    for calculation in available {
        // when do I have all the input params?
        let mut param_fromto: Vec<(i32, OpenTimerange)> = Vec::new();
        for (paramid, fill) in calculation.input_paramids.iter() {
            // for now find the earliest and latest (open) times?
            let first_time = fill.iter().map(|item| item.from).min().unwrap();
            let last_time = if fill.iter().any(|item| item.to.is_none()) {
                // if there is a None to time, that means the series is open ended,
                // which is the latest possible to time. but Option's Ord impl
                // counts None as less than Some. So we have this if check to
                // override that behaviour
                None
            } else {
                fill.iter().map(|item| item.to).max().unwrap()
            };
            param_fromto.push((
                *paramid,
                OpenTimerange {
                    from: Some(first_time),
                    to: last_time,
                },
            ));
        }
        // then find the overlap
        let mut timerange: Option<OpenTimerange> = None;
        for window in param_fromto.windows(2) {
            let prev_timerange = window[0].1;
            let curr_timerange = window[1].1;
            timerange = prev_timerange.overlap(curr_timerange);
        }
        // there is a range where they overlap
        if let Some(timerange) = timerange {
            if let Some(from) = timerange.from {
                let params = AvailableParam {
                    level: calculation.label.level,
                    sensor: calculation.label.sensor,
                    from,
                    to: timerange.to,
                };
                available_calculations
                    .entry((calculation.label.station_id, param_id))
                    .or_default()
                    .push(params);
            }
        }
    }
    // flatten the hashmap into a vec of responses
    let available_calculations_vec = available_calculations
        .into_iter()
        .map(
            |((station_id, param_id), params)| CalculationsAvailableResponse {
                station_id,
                param_id,
                params,
            },
        )
        .collect();

    Ok(Json(available_calculations_vec))
}

async fn calculation_pair_handler(
    pools: DbPools,
    patchwork_tables: PatchworkTables,
    station_id: i32,
    params: CalculationParams,
    roles: Option<(Vec<i32>, Vec<i32>)>,
    param_ids: [i32; 2],
    calculation_fn: impl Fn(f64, f64) -> Result<f64, Error>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
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

    let transform = |obstime, arg1: DataQCtuple, arg2: DataQCtuple| {
        calculation_fn(arg1.value, arg2.value)
            .ok()
            .map(|value| CalculationsResponse {
                timestamp: obstime,
                value,
                underlying_data: Some(
                    [(param_ids[0], arg1), (param_ids[1], arg2)]
                        .into_iter()
                        .collect(),
                ),
            })
    };
    let mut response =
        get_calculation_data_pair(&patches, params.from, params.to, &open_conn, transform)
            .await
            .map_err(error::internal_error)?;
    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}

async fn calculation_triple_handler(
    pools: DbPools,
    patchwork_tables: PatchworkTables,
    station_id: i32,
    params: CalculationParams,
    roles: Option<(Vec<i32>, Vec<i32>)>,
    param_ids: [i32; 3],
    calculation_fn: impl Fn(f64, f64, f64) -> Result<f64, Error>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
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

    let transform = |obstime, arg1: DataQCtuple, arg2: DataQCtuple, arg3: DataQCtuple| {
        calculation_fn(arg1.value, arg2.value, arg3.value)
            .ok()
            .map(|value| CalculationsResponse {
                timestamp: obstime,
                value,
                underlying_data: Some(
                    [
                        (param_ids[0], arg1),
                        (param_ids[1], arg2),
                        (param_ids[2], arg3),
                    ]
                    .into_iter()
                    .collect(),
                ),
            })
    };
    let mut response =
        get_calculation_data_triple(&patches, params.from, params.to, &open_conn, transform)
            .await
            .map_err(error::internal_error)?;
    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}

//#[axum::debug_handler(state = EgressState)]
pub async fn dew_point_temperature_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    calculation_pair_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        roles,
        [211, 262],
        dew_point_temperature,
    )
    .await
}

pub async fn water_vapor_partial_pressure_in_air_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    calculation_pair_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        roles,
        [211, 262],
        water_vapor_partial_pressure_in_air,
    )
    .await
}

pub async fn specific_humidity_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    calculation_triple_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        roles,
        [211, 262, 173],
        specific_humidity,
    )
    .await
}

pub async fn humidity_mixing_ratio_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    calculation_triple_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        roles,
        [211, 262, 173],
        humidity_mixing_ratio,
    )
    .await
}

// TODO: can one have spaces in the path of the routes?
pub fn calculations_router() -> Router<EgressState> {
    Router::new()
        .route("/available/{param_id}", get(calculations_available_handler))
        .route(
            "/217/station/{station_id}",
            get(dew_point_temperature_handler),
        )
        .route("/3123/station/{station_id}", get(specific_humidity_handler))
        .route(
            "/3197/station/{station_id}",
            get(humidity_mixing_ratio_handler),
        )
        .route(
            "/3136/station/{station_id}",
            get(water_vapor_partial_pressure_in_air_handler),
        )
}
