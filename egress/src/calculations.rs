use axum::extract::{Path, Query, State};
use axum::{Json, Router, routing::get};
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::FuturesOrdered};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

use crate::{
    EgressState, Error, PatchworkTables,
    patchwork::PatchworkTimeseriesTable,
    patchwork::{Patch, get_applicable_timeseries},
    util::{CalculationPatch, merge_patches},
};
use ::util::{
    DbPools, PatchworkLabel, PooledPgConn,
    auth::{PermitRoles, StationRoles},
    deserialize::optional_comma_separated,
    http_error::internal,
};

mod humidity;
use humidity::{
    dew_point_temperature, humidity_mixing_ratio, specific_humidity,
    water_vapor_partial_pressure_in_air,
};

pub const CALCULATIONS_REQUESTS_RECEIVED: &str = "calculations_requests_received";
pub const CALCULATIONS_AVAILABLE_REQUESTS_RECEIVED: &str =
    "calculations_available_requests_received";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CalculationParams {
    level: Option<i32>,
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
    #[serde(default, deserialize_with = "optional_comma_separated")]
    accepted_qc: Option<Vec<i32>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CreationParams {
    station_id: i32,
    param_ids: Vec<i32>, // for calculations, we need to specify multiple paramids
    level: Option<i32>,
    sensor: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CalculationResp {
    pub data: Vec<(DateTime<Utc>, f64)>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CalculationAvailable {
    param_id: i32,
    endpoint: String,
}

async fn get_calculation_data_pair(
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    accepted_qc: &[i32],
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
                AND COALESCE(quality_code, -1) = ANY($5::int[])
            ) param1
            INNER JOIN ( 
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $2 
                AND obstime >= $3 AND obstime < $4
                AND COALESCE(quality_code, -1) = ANY($5::int[])
            ) param2
            USING (obstime)"#,
        )
        .await?;

    let mut futures = patches
        .iter()
        .map(|patch| async {
            conn.query(
                &query,
                &[&patch.tsids[0], &patch.tsids[1], &from, &to, &accepted_qc],
            )
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
                data.push((
                    row.get(0),
                    [
                        (val1, row.get::<usize, Option<i32>>(5)),
                        (val2, row.get::<usize, Option<i32>>(6)),
                    ],
                ));
            }
        }
    }

    Ok(data)
}

async fn get_calculation_data_triple(
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    accepted_qc: &[i32],
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
                AND COALESCE(quality_code, -1) = ANY($6::int[])
            ) param1
            INNER JOIN (
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $2
                AND obstime >= $4 AND obstime < $5
                AND COALESCE(quality_code, -1) = ANY($6::int[])
            ) param2
            USING (obstime)
            INNER JOIN (
                SELECT obstime, original, corrected, quality_code FROM legacy.data
                WHERE timeseries = $3
                AND obstime >= $4 AND obstime < $5
                AND COALESCE(quality_code, -1) = ANY($6::int[])
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
                    &accepted_qc,
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
                    (value1.unwrap(), row.get::<usize, Option<i32>>(7)),
                    (value2.unwrap(), row.get::<usize, Option<i32>>(8)),
                    (value3.unwrap(), row.get::<usize, Option<i32>>(9)),
                ],
            ));
        }
    }

    Ok(data)
}

/// Get patches for multiple parameters and merge them.
fn get_merged_patchset(
    parameters: CreationParams,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    permit_roles: &[i32],
    station_roles: &[i32],
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<CalculationPatch>, Error> {
    let patches = parameters
        .param_ids
        .iter()
        .map(|param_id| {
            let patchwork_label = PatchworkLabel {
                station_id: parameters.station_id,
                param_id: *param_id,
                level: parameters.level,
                sensor: parameters.sensor,
            };
            get_applicable_timeseries(
                from,
                to,
                patchwork_label,
                permit_roles,
                station_roles,
                patchwork_table.clone(),
            )
        })
        .collect::<Result<Vec<Vec<Patch>>, Error>>()?;

    Ok(merge_patches(patches))
}

async fn calculation_pair_handler(
    pools: DbPools,
    patchwork_tables: PatchworkTables,
    station_id: i32,
    params: CalculationParams,
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
    patchwork_tables: PatchworkTables,
    station_id: i32,
    params: CalculationParams,
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

/// wrapper to apply a calculation to some data, while coalescing the QC of the data together
fn apply_calc2(
    timestamp: DateTime<Utc>,
    data: [(f64, Option<i32>); 2],
    f: impl Fn(f64, f64) -> f64,
) -> (DateTime<Utc>, f64) {
    // apply the calculation function to the values of the two params
    // choosing the corrected value if it exists, otherwise the original value
    let value = match (data[0].1.is_some(), data[1].1.is_some()) {
        (true, true) => f(data[0].1.unwrap() as f64, data[1].1.unwrap() as f64),
        (true, false) => f(data[0].1.unwrap() as f64, data[1].0),
        (false, true) => f(data[0].0, data[1].1.unwrap() as f64),
        (false, false) => f(data[0].0, data[1].0),
    };
    (timestamp, value)
}

/// see `[apply_calc2]` but for calculations that take 3 inputs
fn apply_calc3(
    timestamp: DateTime<Utc>,
    data: [(f64, Option<i32>); 3],
    f: impl Fn(f64, f64, f64) -> f64,
) -> (DateTime<Utc>, f64) {
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
    (timestamp, value)
}

// makes paramid 217 (dew point temperature) from 211 (temperature) and 262 (relative humidity)
pub async fn dew_point_temperature_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    PermitRoles(permit_roles): PermitRoles,
    StationRoles(station_roles): StationRoles,
) -> Result<Json<CalculationResp>, (StatusCode, String)> {
    // get the data (either from open or restricted - depending on if open exists, and if user has access to restricted)
    let data = calculation_pair_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        &permit_roles,
        &station_roles,
        [211, 262],
    )
    .await?;

    // do the calculation and coalesce the QC together
    let result: Vec<(DateTime<Utc>, f64)> = data
        .into_iter()
        .map(|(time, d)| apply_calc2(time, d, dew_point_temperature))
        .collect();

    Ok(Json(CalculationResp { data: result }))
}

// makes paramid 3136 (water vapor partial pressure in air) from 211 (temperature) and 262 (relative humidity)
pub async fn water_vapor_partial_pressure_in_air_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    PermitRoles(permit_roles): PermitRoles,
    StationRoles(station_roles): StationRoles,
) -> Result<Json<CalculationResp>, (StatusCode, String)> {
    // get the data (either from open or restricted - depending on if open exists, and if user has access to restricted)
    let data = calculation_pair_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        &permit_roles,
        &station_roles,
        [211, 262],
    )
    .await?;

    // do the calculation and coalesce the QC together
    let result: Vec<(DateTime<Utc>, f64)> = data
        .into_iter()
        .map(|(time, d)| apply_calc2(time, d, water_vapor_partial_pressure_in_air))
        .collect();

    Ok(Json(CalculationResp { data: result }))
}

// makes 3123 (specific humidity) from 211 (temperature), 262 (relative humidity), and 173 (pressure)
pub async fn specific_humidity_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    PermitRoles(permit_roles): PermitRoles,
    StationRoles(station_roles): StationRoles,
) -> Result<Json<CalculationResp>, (StatusCode, String)> {
    // get the data (either from open or restricted - depending on if open exists, and if user has access to restricted)
    let data = calculation_triple_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        &permit_roles,
        &station_roles,
        [211, 262, 173],
    )
    .await?;

    // do the calculation and coalesce the QC together
    let result: Vec<(DateTime<Utc>, f64)> = data
        .into_iter()
        .map(|(time, d)| apply_calc3(time, d, specific_humidity))
        .collect();

    Ok(Json(CalculationResp { data: result }))
}

// makes 3197 (humidity mixing ratio) from 211 (temperature), 262 (relative humidity), and 173 (pressure)
pub async fn humidity_mixing_ratio_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    PermitRoles(permit_roles): PermitRoles,
    StationRoles(station_roles): StationRoles,
) -> Result<Json<CalculationResp>, (StatusCode, String)> {
    // get the data (either from open or restricted - depending on if open exists, and if user has access to restricted)
    let data = calculation_triple_handler(
        pools,
        patchwork_tables,
        station_id,
        params,
        &permit_roles,
        &station_roles,
        [211, 262, 173],
    )
    .await?;

    // do the calculation and coalesce the QC together
    let result: Vec<(DateTime<Utc>, f64)> = data
        .into_iter()
        .map(|(time, d)| apply_calc3(time, d, humidity_mixing_ratio))
        .collect();

    Ok(Json(CalculationResp { data: result }))
}

// this endpoint is just for listing what sub endpoints exist for calculations
pub async fn calculation_availability_handler()
-> Result<Json<Vec<CalculationAvailable>>, (StatusCode, String)> {
    // NOTE: this list should be kept up to date with the implemented calculations
    let response = [217, 3123, 3197, 3136]
        .iter()
        .map(|p| CalculationAvailable {
            param_id: *p,
            endpoint: format! {"calculations/station/{{station_id}}/{p}"},
        })
        .collect();
    Ok(Json(response))
}

pub fn calculations_router() -> Router<EgressState> {
    Router::new()
        .route("/params", get(calculation_availability_handler))
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
