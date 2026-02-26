use axum::{routing::get, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error;
use crate::error::Error;
use crate::patchwork;
use crate::patchwork::PatchworkLabel;
use crate::patchwork::{Fill, PatchworkDatum, PatchworkTimeseriesTable};
use crate::EgressState;
use crate::PatchworkTables;
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Timelike, Utc};
use http::StatusCode;
use util::{DbPools, OpenTimerange, PooledPgConn};

use crate::calculations::humidity::{
    dew_point_temperature, humidity_mixing_ratio, specific_humidity,
    water_vapor_partial_pressure_in_air,
};

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub struct ProductParams {
    stationid: i32,
    level: Option<i32>,
    sensor: i32,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductsAvailableResponse {
    param_id: i32,
    station_id: i32,
    level: Option<i32>,
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataQCtuple {
    value: f64,
    quality_code: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductsResponse {
    param_id: i32,
    timestamp: DateTime<Utc>,
    value: f64,
    underlying_data: Option<HashMap<i32, DataQCtuple>>, // paramid -> (value, quality_code)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductsConstructor {
    label: PotentialProductsLabel,
    input_paramids: Vec<(i32, Vec<Fill>)>,
}

// label needed for sorting if the patchwork labels have all
// the input paramids for a product / calculation
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct PotentialProductsLabel {
    pub station_id: i32,
    pub level: Option<i32>,
    pub sensor: Option<i32>,
}

pub fn available_products_for_param(
    param_id: i32,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<ProductsConstructor>, Error> {
    match param_id {
        // "dew_point_temperature"
        217 => get_param_products(vec![211, 262], patchwork_table),
        // "specific_humidity"
        3123 => get_param_products(vec![211, 262, 173], patchwork_table),
        // "over_time(humidity_mixing_ratio P1D)"
        3197 => get_param_products(vec![211, 262, 173], patchwork_table),
        // "mean(water_vapor_partial_pressure_in_air P1D)"
        3136 => get_param_products(vec![211, 262], patchwork_table),
        _ => Err(Error::InvalidParam(param_id.to_string())),
    }
}

fn get_param_products(
    input_paramids: Vec<i32>,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<ProductsConstructor>, Error> {
    let mut param_available: Vec<ProductsConstructor> = Vec::new();

    // just do the open table for now
    let table_guard = patchwork_table
        .read()
        .map_err(|e| Error::Lock(e.to_string()))?;

    let mut found_params: HashMap<PotentialProductsLabel, Vec<(i32, Vec<Fill>)>> = HashMap::new();
    // iterate over all the labels in the patchwork table
    for (key, value) in table_guard.iter() {
        if key.station_id > 99999 {
            // skip data from outside Norway
            continue;
        }
        // for each product, keep anything that could be an input param
        if input_paramids[0..].contains(&key.param_id) {
            let label = PotentialProductsLabel {
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
    // if have all the input params for the product, then add to available products
    // TODO: check the time range... cut down to overlap!
    for (key, value) in found_params.iter() {
        // actually have all the input parameters?
        if value.len() == input_paramids.len() {
            // add to the product table
            param_available.push(ProductsConstructor {
                label: key.clone(),
                input_paramids: value.clone(),
            });
        }
    }
    drop(table_guard); // release the read lock
    Ok(param_available)
}

// helper functions ...
fn _is_ten_min_freq(dt: &DateTime<Utc>) -> bool {
    (dt.minute() == 0
        || dt.minute() == 10
        || dt.minute() == 20
        || dt.minute() == 30
        || dt.minute() == 40
        || dt.minute() == 50)
        && dt.second() == 0
        && dt.nanosecond() == 0
}

async fn get_data_single(
    paramid: i32,
    params: ProductParams,
    patchwork_tables: PatchworkTables,
    conn: &PooledPgConn<'_>,
) -> Result<Vec<patchwork::PatchworkDatum>, (StatusCode, String)> {
    let label = PatchworkLabel {
        station_id: params.stationid,
        param_id: paramid,
        sensor: Some(params.sensor),
        level: params.level,
    };
    // try to get the data from patchwork
    let data: Vec<patchwork::PatchworkDatum> = patchwork::get_patchwork(
        conn,
        params.from,
        params.to,
        label,
        patchwork_tables.open.clone(),
        None, // TODO: not handling auth here
    )
    .await
    .map_err(error::internal_error)?;
    Ok(data)
}

async fn get_vec_data_pair(
    data0: Vec<patchwork::PatchworkDatum>,
    data1: Vec<patchwork::PatchworkDatum>,
) -> Result<Vec<(DateTime<Utc>, DataQCtuple, DataQCtuple)>, (StatusCode, String)> {
    // splice the data together based on timestamp, so have a vector of (timestamp, data0, data1)
    // only keep the timestamps where have both data0 and data1
    let mut data_pair: Vec<(DateTime<Utc>, DataQCtuple, DataQCtuple)> = Vec::new();
    let mut iter0 = data0.iter();
    let mut iter1 = data1.iter();
    let iters = [&mut iter0, &mut iter1];
    // start on the first iterator
    let mut curr = 0;
    // if nothing here it will just skip the loop and return an empty vector, which is what we want
    if let Some(mut item_outer) = iters[curr].next() {
        let mut timestamp = item_outer.timestamp;
        curr = (curr + 1) % 2; // switch to the other iterator
                               // but within here we need to be able to switch which iterator we are on
        for item_inner in iters[curr].by_ref() {
            if item_inner.timestamp == timestamp {
                // if the timestamps match, we have a pair, so add to the data_pair vector
                let pair = unwrap_data_pair(Some(item_outer), Some(item_inner))
                    .map_err(error::internal_error)?;
                if let Some((p0, p1)) = pair {
                    data_pair.push((
                        timestamp,
                        DataQCtuple {
                            value: p0,
                            quality_code: item_outer.quality_code,
                        },
                        DataQCtuple {
                            value: p1,
                            quality_code: item_inner.quality_code,
                        },
                    ));
                }
                break; // break out of the inner loop and go back to the outer loop
            } else if item_inner.timestamp < timestamp {
                // still less than the timestamp we are looking for, so keep iterating through this inner iterator
                continue;
            } else if item_inner.timestamp > timestamp {
                // set the timestamp to this and switch the iterators
                timestamp = item_inner.timestamp;
                item_outer = item_inner; // set the outer item to this new timestamp item
                curr = (curr + 1) % 2; // switch to the other iterator
            }
        }
    }
    Ok(data_pair)
}

async fn get_vec_data_triple(
    data0: Vec<patchwork::PatchworkDatum>,
    data1: Vec<patchwork::PatchworkDatum>,
    data2: Vec<patchwork::PatchworkDatum>,
) -> Result<Vec<(DateTime<Utc>, DataQCtuple, DataQCtuple, DataQCtuple)>, (StatusCode, String)> {
    // splice the data together based on timestamp, so have a vector of (timestamp, data0, data1, data2)
    // only keep the timestamps where have all three data points
    let mut data_pair: Vec<(DateTime<Utc>, DataQCtuple, DataQCtuple, DataQCtuple)> = Vec::new();
    for d0 in data0 {
        let timestamp = d0.timestamp;
        let d1 = data1.iter().find(|d| d.timestamp == timestamp);
        let d2 = data2.iter().find(|d| d.timestamp == timestamp);
        let pair = unwrap_data_triple(Some(&d0), d1, d2).map_err(error::internal_error)?;
        if let Some((p0, p1, p2)) = pair {
            data_pair.push((
                timestamp,
                DataQCtuple {
                    value: p0,
                    quality_code: d0.quality_code,
                },
                DataQCtuple {
                    value: p1,
                    quality_code: d1.and_then(|d| d.quality_code),
                },
                DataQCtuple {
                    value: p2,
                    quality_code: d2.and_then(|d| d.quality_code),
                },
            ));
        }
    }
    Ok(data_pair)
}

fn unwrap_data_pair(
    data1: Option<&PatchworkDatum>,
    data2: Option<&PatchworkDatum>,
) -> Result<Option<(f64, f64)>, Error> {
    // deal with unwrapping the options, choosing correct if exists, or else original
    match (data1, data2) {
        (Some(data1), Some(data2)) => {
            match (
                data1.corrected,
                data1.original,
                data2.corrected,
                data2.original,
            ) {
                (Some(data1_corr), Some(_), Some(data2_corr), Some(_)) => {
                    Ok(Some((data1_corr, data2_corr)))
                }
                (None, Some(data1_orig), None, Some(data2_orig)) => {
                    Ok(Some((data1_orig, data2_orig)))
                }
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

fn unwrap_data_triple(
    data1: Option<&PatchworkDatum>,
    data2: Option<&PatchworkDatum>,
    data3: Option<&PatchworkDatum>,
) -> Result<Option<(f64, f64, f64)>, Error> {
    // deal with unwrapping the options, choosing correct if exists, or else original
    match (data1, data2, data3) {
        (Some(data1), Some(data2), Some(data3)) => {
            match (
                data1.corrected,
                data1.original,
                data2.corrected,
                data2.original,
                data3.corrected,
                data3.original,
            ) {
                (
                    Some(data1_corr),
                    Some(_),
                    Some(data2_corr),
                    Some(_),
                    Some(data3_corr),
                    Some(_),
                ) => Ok(Some((data1_corr, data2_corr, data3_corr))),
                (None, Some(data1_orig), None, Some(data2_orig), None, Some(data3_orig)) => {
                    Ok(Some((data1_orig, data2_orig, data3_orig)))
                }
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

pub async fn products_available_handler(
    Path(param_id): Path<i32>,
    State(patchwork_tables): State<PatchworkTables>,
) -> Result<Json<Vec<ProductsAvailableResponse>>, (StatusCode, String)> {
    // TODO:
    // Make it work for more than the open timeseries
    let available: Vec<ProductsConstructor> =
        available_products_for_param(param_id, patchwork_tables.open)
            .map_err(error::internal_error)?;
    let mut available_products: Vec<ProductsAvailableResponse> = Vec::new();
    for product in available {
        // when do I have all the input params?
        let mut param_fromto: Vec<(i32, OpenTimerange)> = Vec::new();
        for (paramid, fill) in product.input_paramids.iter() {
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
                available_products.push(ProductsAvailableResponse {
                    param_id,
                    station_id: product.label.station_id,
                    level: product.label.level,
                    sensor: product.label.sensor,
                    from,
                    to: timerange.to,
                });
            }
        }
    }

    Ok(Json(available_products))
}

//#[axum::debug_handler(state = EgressState)]
pub async fn dew_point_temperature_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<ProductParams>,
) -> Result<Json<Vec<ProductsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<ProductsResponse> = Vec::new();

    // labels to get data for: 211, 262
    let data_211 = get_data_single(211, params, patchwork_tables.clone(), &open_conn).await?;
    let data_262 = get_data_single(262, params, patchwork_tables.clone(), &open_conn).await?;
    // see if we have the two values
    let data_pair = get_vec_data_pair(data_211, data_262).await?;
    for (time, air_temperature, relative_humidity) in data_pair.into_iter() {
        let value = dew_point_temperature(air_temperature.value, relative_humidity.value).unwrap();
        response.push(ProductsResponse {
            param_id: 217,
            timestamp: time,
            value,
            underlying_data: Some(
                vec![
                    (
                        211,
                        DataQCtuple {
                            value: air_temperature.value,
                            quality_code: air_temperature.quality_code,
                        },
                    ),
                    (
                        262,
                        DataQCtuple {
                            value: relative_humidity.value,
                            quality_code: relative_humidity.quality_code,
                        },
                    ),
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

pub async fn specific_humidity_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<ProductParams>,
) -> Result<Json<Vec<ProductsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<ProductsResponse> = Vec::new();

    // labels to get data for: 211, 262, 173
    let data_211 = get_data_single(211, params, patchwork_tables.clone(), &open_conn).await?;
    let data_262 = get_data_single(262, params, patchwork_tables.clone(), &open_conn).await?;
    let data_173 = get_data_single(173, params, patchwork_tables.clone(), &open_conn).await?;
    // see if we have the two values
    let data_pair = get_vec_data_triple(data_211, data_262, data_173).await?;
    for (time, air_temperature, relative_humidity, surface_air_pressure) in data_pair.into_iter() {
        let value = specific_humidity(
            air_temperature.value,
            relative_humidity.value,
            surface_air_pressure.value,
        )
        .unwrap();
        response.push(ProductsResponse {
            param_id: 3123,
            timestamp: time,
            value,
            underlying_data: Some(
                vec![
                    (
                        211,
                        DataQCtuple {
                            value: air_temperature.value,
                            quality_code: air_temperature.quality_code,
                        },
                    ),
                    (
                        262,
                        DataQCtuple {
                            value: relative_humidity.value,
                            quality_code: relative_humidity.quality_code,
                        },
                    ),
                    (
                        173,
                        DataQCtuple {
                            value: surface_air_pressure.value,
                            quality_code: surface_air_pressure.quality_code,
                        },
                    ),
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

pub async fn humidity_mixing_ratio_router(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<ProductParams>,
) -> Result<Json<Vec<ProductsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<ProductsResponse> = Vec::new();

    // labels to get data for: 211, 262, 173
    let data_211 = get_data_single(211, params, patchwork_tables.clone(), &open_conn).await?;
    let data_262 = get_data_single(262, params, patchwork_tables.clone(), &open_conn).await?;
    let data_173 = get_data_single(173, params, patchwork_tables.clone(), &open_conn).await?;
    // see if we have the two values
    let data_pair = get_vec_data_triple(data_211, data_262, data_173).await?;
    for (time, air_temperature, relative_humidity, surface_air_pressure) in data_pair.into_iter() {
        let value = humidity_mixing_ratio(
            air_temperature.value,
            relative_humidity.value,
            surface_air_pressure.value,
        )
        .unwrap();
        response.push(ProductsResponse {
            param_id: 3197,
            timestamp: time,
            value,
            underlying_data: Some(
                vec![
                    (
                        211,
                        DataQCtuple {
                            value: air_temperature.value,
                            quality_code: air_temperature.quality_code,
                        },
                    ),
                    (
                        262,
                        DataQCtuple {
                            value: relative_humidity.value,
                            quality_code: relative_humidity.quality_code,
                        },
                    ),
                    (
                        173,
                        DataQCtuple {
                            value: surface_air_pressure.value,
                            quality_code: surface_air_pressure.quality_code,
                        },
                    ),
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

pub async fn water_vapor_partial_pressure_in_air_router(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<ProductParams>,
) -> Result<Json<Vec<ProductsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut response: Vec<ProductsResponse> = Vec::new();
    // labels to get data for: 211, 262
    let data_211 = get_data_single(211, params, patchwork_tables.clone(), &open_conn).await?;
    let data_262 = get_data_single(262, params, patchwork_tables.clone(), &open_conn).await?;
    // see if we have the two values
    let data_pair = get_vec_data_pair(data_211, data_262).await?;
    for (time, air_temperature, relative_humidity) in data_pair.into_iter() {
        let value =
            water_vapor_partial_pressure_in_air(air_temperature.value, relative_humidity.value)
                .unwrap();
        response.push(ProductsResponse {
            param_id: 3136,
            timestamp: time,
            value,
            underlying_data: Some(
                vec![
                    (
                        211,
                        DataQCtuple {
                            value: air_temperature.value,
                            quality_code: air_temperature.quality_code,
                        },
                    ),
                    (
                        262,
                        DataQCtuple {
                            value: relative_humidity.value,
                            quality_code: relative_humidity.quality_code,
                        },
                    ),
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

// TODO: can one have spaces in the path of the routes?
pub fn products_router() -> Router<EgressState> {
    Router::new()
        .route("/available/{param_id}", get(products_available_handler))
        .route("/217", get(dew_point_temperature_handler))
        .route("/3123", get(specific_humidity_handler))
        .route("/3197", get(humidity_mixing_ratio_router))
        .route("/3136", get(water_vapor_partial_pressure_in_air_router))
}
