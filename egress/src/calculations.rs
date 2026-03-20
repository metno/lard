use axum::extract::{Path, Query, State};
use axum::{routing::get, Extension, Json, Router};
use chrono::{DateTime, Utc};
use futures::{stream::FuturesOrdered, StreamExt};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error;
use crate::error::Error;
use crate::patchwork;
use crate::patchwork::{Fill, Patch, PatchworkTimeseriesTable};
use crate::EgressState;
use crate::PatchworkTables;
use util::{ClosedTimerange, DbPools, OpenTimerange, PatchworkLabel, PooledPgConn};
mod humidity;
use crate::calculations::humidity::{
    dew_point_temperature, humidity_mixing_ratio, specific_humidity,
    water_vapor_partial_pressure_in_air,
};

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

// recursive function
fn merge_patches(
    mut patches_to_merge: Vec<Vec<Patch>>,
    mut patches: Vec<CalculationPatch>,
) -> Vec<CalculationPatch> {
    // check if we need to iterate... (otherwise end of recursion and return merged patches at the end of the function)
    if !patches_to_merge.is_empty() {
        if patches.is_empty() {
            // first iteration, so just add the first patch as a starting point for the merge
            patches.push(CalculationPatch {
                tsids: vec![patches_to_merge[0][0].tsid],
                from: patches_to_merge[0][0].from,
                to: patches_to_merge[0][0].to,
            });
            patches_to_merge = patches_to_merge[1..].to_vec(); // remove the first patch since we have added it to the merged patches
        }

        // merge the next patch with the existing patches
        for p in patches.iter_mut() {
            let p_time = ClosedTimerange {
                from: p.from,
                to: p.to,
            };
            for np in patches_to_merge[0].iter() {
                let np_time = ClosedTimerange {
                    from: np.from,
                    to: np.to,
                };
                if let Some(overlap) = p_time.overlap(np_time) {
                    // if there is an overlap, modify the current patch
                    p.from = overlap.from;
                    p.to = overlap.to;
                    p.tsids.push(np.tsid);
                }
            }
        }
        // recursively merge the rest of the patches
        merge_patches(patches_to_merge[1..].to_vec(), patches.clone());
    }

    // reached the end of iteration (recursive) or there were no patches to merge in the first place
    patches // return the merged patches
}

pub fn available_calculations_for_param(
    param_id: i32,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<CalculationsConstructor>, Error> {
    match param_id {
        // "dew_point_temperature"
        217 => get_param_calculations(vec![211, 262], patchwork_table),
        // "specific_humidity"
        3123 => get_param_calculations(vec![211, 262, 173], patchwork_table),
        // "over_time(humidity_mixing_ratio P1D)"
        3197 => get_param_calculations(vec![211, 262, 173], patchwork_table),
        // "mean(water_vapor_partial_pressure_in_air P1D)"
        3136 => get_param_calculations(vec![211, 262], patchwork_table),
        _ => Err(Error::InvalidParam(param_id.to_string())),
    }
}

async fn get_calculation_data_pair(
    patches: &[CalculationPatch],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    conn: &PooledPgConn<'_>,
) -> Result<Vec<(DateTime<Utc>, DataQCtuple, DataQCtuple)>, Error> {
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
                data.push((row.get(0), d1, d2));
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
) -> Result<Vec<(DateTime<Utc>, DataQCtuple, DataQCtuple, DataQCtuple)>, Error> {
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
            data.push((row.get(0), d1, d2, d3));
        }
    }

    Ok(data)
}

fn get_param_calculations(
    input_paramids: Vec<i32>,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<CalculationsConstructor>, Error> {
    let mut param_available: Vec<CalculationsConstructor> = Vec::new();

    // just do the open table for now
    let table_guard = patchwork_table.read()?;

    let mut found_params: HashMap<PotentialCalculationsLabel, Vec<(i32, Vec<Fill>)>> =
        HashMap::new();
    // iterate over all the labels in the patchwork table
    for (key, value) in table_guard.iter() {
        if key.station_id > 99999 {
            // skip data from outside Norway
            continue;
        }
        // for each calculation, keep anything that could be an input param
        if input_paramids[0..].contains(&key.param_id) {
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
    // if have all the input params for the calculation, then add to available calculations
    // TODO: check the time range... cut down to overlap!
    for (key, value) in found_params.iter() {
        // actually have all the input parameters?
        if value.len() == input_paramids.len() {
            // add to the calculation table
            param_available.push(CalculationsConstructor {
                label: key.clone(),
                input_paramids: value.clone(),
            });
        }
    }
    drop(table_guard); // release the read lock
    Ok(param_available)
}

pub async fn calculations_available_handler(
    Path(param_id): Path<i32>,
    State(patchwork_tables): State<PatchworkTables>,
    Extension(_roles): Extension<Option<(Vec<i32>, Vec<i32>)>>, // TODO: use the roles for the closed table
) -> Result<Json<Vec<CalculationsAvailableResponse>>, (StatusCode, String)> {
    // TODO:
    // Make it work for more than the open timeseries
    let available: Vec<CalculationsConstructor> =
        available_calculations_for_param(param_id, patchwork_tables.open)
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

//#[axum::debug_handler(state = EgressState)]
pub async fn dew_point_temperature_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<CalculationsResponse> = Vec::new();

    // labels to get data for: 211, 262
    let (roles_permit, roles_station) = roles.unwrap_or_default();
    let label_211 = PatchworkLabel {
        station_id,
        param_id: 211,
        level: params.level,
        sensor: params.sensor,
    };
    let label_262 = PatchworkLabel {
        station_id,
        param_id: 262,
        level: params.level,
        sensor: params.sensor,
    };
    let patches_211 = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label_211,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;
    let patches_262 = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label_262,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;

    let patches_vec = vec![patches_211.clone(), patches_262.clone()];
    let patches = merge_patches(patches_vec, vec![]);

    let data = get_calculation_data_pair(&patches, params.from, params.to, &open_conn)
        .await
        .map_err(error::internal_error)?;
    for (obstime, air_temperature, relative_humidity) in data {
        let value = dew_point_temperature(air_temperature.value, relative_humidity.value).unwrap();
        response.push(CalculationsResponse {
            timestamp: obstime,
            value,
            underlying_data: Some(
                vec![(211, air_temperature), (262, relative_humidity)]
                    .into_iter()
                    .collect(),
            ),
        });
    }

    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}

pub async fn specific_humidity_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<CalculationsResponse> = Vec::new();

    // labels to get data for: 211, 262, 173
    let (roles_permit, roles_station) = roles.unwrap_or_default();
    let label_211 = PatchworkLabel {
        station_id,
        param_id: 211,
        level: params.level,
        sensor: params.sensor,
    };
    let label_262 = PatchworkLabel {
        station_id,
        param_id: 262,
        level: params.level,
        sensor: params.sensor,
    };
    let label_173 = PatchworkLabel {
        station_id,
        param_id: 173,
        level: params.level,
        sensor: params.sensor,
    };
    let patches_211 = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label_211,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;
    let patches_262 = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label_262,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;
    let patches_173 = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label_173,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;

    let patches_vec = vec![
        patches_211.clone(),
        patches_262.clone(),
        patches_173.clone(),
    ];
    let patches = merge_patches(patches_vec, vec![]);

    let data = get_calculation_data_triple(&patches, params.from, params.to, &open_conn)
        .await
        .map_err(error::internal_error)?;
    for (obstime, air_temperature, relative_humidity, surface_air_pressure) in data {
        let value = specific_humidity(
            air_temperature.value,
            relative_humidity.value,
            surface_air_pressure.value,
        )
        .unwrap();
        response.push(CalculationsResponse {
            timestamp: obstime,
            value,
            underlying_data: Some(
                vec![
                    (211, air_temperature),
                    (262, relative_humidity),
                    (173, surface_air_pressure),
                ]
                .into_iter()
                .collect(),
            ),
        });
    }

    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}

pub async fn humidity_mixing_ratio_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<CalculationsResponse> = Vec::new();

    // labels to get data for: 211, 262, 173
    let (roles_permit, roles_station) = roles.unwrap_or_default();
    let label_211 = PatchworkLabel {
        station_id,
        param_id: 211,
        level: params.level,
        sensor: params.sensor,
    };
    let label_262 = PatchworkLabel {
        station_id,
        param_id: 262,
        level: params.level,
        sensor: params.sensor,
    };
    let label_173 = PatchworkLabel {
        station_id,
        param_id: 173,
        level: params.level,
        sensor: params.sensor,
    };
    let patches_211 = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label_211,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;
    let patches_262 = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label_262,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;
    let patches_173 = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label_173,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;

    let patches_vec = vec![
        patches_211.clone(),
        patches_262.clone(),
        patches_173.clone(),
    ];
    let patches = merge_patches(patches_vec, vec![]);

    let data = get_calculation_data_triple(&patches, params.from, params.to, &open_conn)
        .await
        .map_err(error::internal_error)?;
    for (obstime, air_temperature, relative_humidity, surface_air_pressure) in data {
        let value = humidity_mixing_ratio(
            air_temperature.value,
            relative_humidity.value,
            surface_air_pressure.value,
        )
        .unwrap();
        response.push(CalculationsResponse {
            timestamp: obstime,
            value,
            underlying_data: Some(
                vec![
                    (211, air_temperature),
                    (262, relative_humidity),
                    (173, surface_air_pressure),
                ]
                .into_iter()
                .collect(),
            ),
        });
    }
    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}

pub async fn water_vapor_partial_pressure_in_air_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Path(station_id): Path<i32>,
    Query(params): Query<CalculationParams>,
    Extension(roles): Extension<Option<(Vec<i32>, Vec<i32>)>>,
) -> Result<Json<Vec<CalculationsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<CalculationsResponse> = Vec::new();
    // labels to get data for: 211, 262
    let (roles_permit, roles_station) = roles.unwrap_or_default();
    let label_211 = PatchworkLabel {
        station_id,
        param_id: 211,
        level: params.level,
        sensor: params.sensor,
    };
    let label_262 = PatchworkLabel {
        station_id,
        param_id: 262,
        level: params.level,
        sensor: params.sensor,
    };
    let patches_211 = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label_211,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;
    let patches_262 = patchwork::get_applicable_timeseries(
        params.from,
        params.to,
        label_262,
        &roles_permit,
        &roles_station,
        patchwork_tables.open.clone(),
    )
    .map_err(error::internal_error)?;

    let patches_vec = vec![patches_211.clone(), patches_262.clone()];
    let patches = merge_patches(patches_vec, vec![]);

    let data = get_calculation_data_pair(&patches, params.from, params.to, &open_conn)
        .await
        .map_err(error::internal_error)?;
    for (obstime, air_temperature, relative_humidity) in data {
        let value =
            water_vapor_partial_pressure_in_air(air_temperature.value, relative_humidity.value)
                .unwrap();
        response.push(CalculationsResponse {
            timestamp: obstime,
            value,
            underlying_data: Some(
                vec![(211, air_temperature), (262, relative_humidity)]
                    .into_iter()
                    .collect(),
            ),
        });
    }

    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
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
