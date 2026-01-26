use axum::{routing::get, Router};
use csv::ReaderBuilder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, RwLock};

use crate::error;
use crate::error::Error;
use crate::patchwork;
use crate::patchwork::PatchworkLabel;
use crate::patchwork::{Fill, PatchworkTimeseriesTable};
use crate::EgressState;
use crate::PatchworkTables;
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Timelike, Utc};
use http::StatusCode;
use tracing::error;
use util::{DbPools, OpenTimerange};

use crate::calculations::humidity::{
    dew_point_temperature, humidity_mixing_ratio, specific_humidity,
    water_vapor_partial_pressure_in_air,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductParams {
    stationid: i32,
    level: Option<i32>,
    sensor: i32,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductsAvailableResponse {
    element: String,
    station_id: i32,
    level: Option<i32>,
    sensor: Option<i32>,
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductsResponse {
    name: String,
    timestamp: DateTime<Utc>,
    value: f64,
    underlying_data: Option<HashMap<i32, (f64, Option<i32>)>>, // paramid -> (value, quality_code)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CalculationsDatum {
    paramid: i32,
    original: Option<f64>,
    corrected: Option<f64>,
    quality_code: Option<i32>,
}

// define a struct for products
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ProductParse {
    pub input_paramids: String,
    pub output_paramid: i32,
    #[serde(rename = "element_id")]
    pub element: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Product {
    pub input_paramids: Vec<i32>,
    pub output_paramid: i32,
    pub element: String,
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

pub fn load_product_list(filename: &str) -> Result<Vec<Product>, Error> {
    let mut list: Vec<Product> = Vec::new();

    // TODO: avoid the unwrap here???
    let file = File::open(filename).unwrap();
    let mut rdr = ReaderBuilder::new().delimiter(b';').from_reader(file);

    rdr.deserialize().for_each(|result| {
        let record: ProductParse = result.unwrap();

        let parsed_vector: Vec<i32> = record
            .input_paramids
            .trim_matches(|c| c == '[' || c == ']') // Remove brackets if present
            .split(',')
            .filter_map(|s| s.trim().parse().ok()) // Parse each element
            .collect();

        list.push(Product {
            input_paramids: parsed_vector,
            output_paramid: record.output_paramid,
            element: record.element,
        });
    });
    Ok(list)
}

pub fn available_products_for_element(
    element: &str,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<ProductsConstructor>, Error> {
    match element {
        "dew_point_temperature" => get_element_products(vec![211, 262], patchwork_table),
        "specific_humidity" => get_element_products(vec![211, 262, 173], patchwork_table),
        "over_time(humidity_mixing_ratio P1D)" => {
            get_element_products(vec![211, 262, 173], patchwork_table)
        }
        "mean(water_vapor_partial_pressure_in_air P1D)" => {
            get_element_products(vec![211, 262], patchwork_table)
        }
        _ => Err(Error::InvalidElement(element.to_string())),
    }
}

fn get_element_products(
    input_paramids: Vec<i32>,
    patchwork_table: Arc<RwLock<PatchworkTimeseriesTable>>,
) -> Result<Vec<ProductsConstructor>, Error> {
    let mut element_available: Vec<ProductsConstructor> = Vec::new();

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
            element_available.push(ProductsConstructor {
                label: key.clone(),
                input_paramids: value.clone(),
            });
        }
    }
    drop(table_guard); // release the read lock
    Ok(element_available)
}

// helper functions ...
fn is_ten_min_freq(dt: &DateTime<Utc>) -> bool {
    (dt.minute() == 0
        || dt.minute() == 10
        || dt.minute() == 20
        || dt.minute() == 30
        || dt.minute() == 40
        || dt.minute() == 50)
        && dt.second() == 0
        && dt.nanosecond() == 0
}

fn unwrap_data_pair(
    data1: Option<&CalculationsDatum>,
    data2: Option<&CalculationsDatum>,
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
    data1: Option<&CalculationsDatum>,
    data2: Option<&CalculationsDatum>,
    data3: Option<&CalculationsDatum>,
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
    Path(element_id): Path<String>,
    State(patchwork_tables): State<PatchworkTables>,
) -> Result<Json<Vec<ProductsAvailableResponse>>, (StatusCode, String)> {
    // TODO:
    // Make it work for more than the open timeseries
    let available: Vec<ProductsConstructor> =
        available_products_for_element(&element_id, patchwork_tables.open)
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
                    element: element_id.clone(),
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
pub async fn products_handler(
    State(pools): State<DbPools>,
    Path(element_id): Path<String>,
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<ProductParams>,
) -> Result<Json<Vec<ProductsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut data: HashMap<DateTime<Utc>, Vec<CalculationsDatum>> = HashMap::new();
    // reduce the patchwork tables to only those matching the requested stationid
    let station_patch_open: PatchworkTimeseriesTable = patchwork_tables
        .open
        .read()
        .map_err(error::internal_error)?
        .iter()
        .filter(|(label, _)| label.station_id == params.stationid)
        .map(|(label, fills)| (*label, fills.clone()))
        .collect();
    let lock_station_patch_open = Arc::new(RwLock::new(station_patch_open));
    let available: Vec<ProductsConstructor> =
        available_products_for_element(&element_id, lock_station_patch_open.clone())
            .map_err(error::internal_error)?;
    if available.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            format!(
                "No available products for element_id: {} and station: {}",
                element_id, params.stationid
            ),
        ));
    }

    // filter to only those matching the requested stationid, level, sensor
    let filtered: Vec<ProductsConstructor> = available
        .into_iter()
        .filter(|p| {
            p.label.station_id == params.stationid
                && (p.label.level == params.level || params.level.is_none()) // levels can be null
                && p.label.sensor == Some(params.sensor)
        })
        .collect();
    for ts in filtered.iter() {
        for p in ts.input_paramids.iter() {
            let label = PatchworkLabel {
                station_id: ts.label.station_id,
                level: ts.label.level,
                sensor: ts.label.sensor,
                param_id: p.0,
            };
            let d = patchwork::get_patchwork(
                &open_conn,
                params.from,
                params.to,
                label,
                patchwork_tables.open.clone(),
                None,
            )
            .await
            .map_err(error::internal_error)?;
            // this should be all the data for one of the input paramids for this product
            // filter out the non ten minute frequency data???
            for x in d {
                if is_ten_min_freq(&x.timestamp) {
                    // add to the hashmap
                    data.entry(x.timestamp)
                        .or_default()
                        .push(CalculationsDatum {
                            paramid: p.0,
                            original: x.original,
                            corrected: x.corrected,
                            quality_code: x.quality_code,
                        });
                }
            }
        }
    }

    let mut response: Vec<ProductsResponse> = Vec::new();

    // do the calculation
    for (time, vector) in data {
        // if have the same timestamp for the input paramids
        match element_id.as_str() {
            // TODO: make a massive match statement with all the names
            // could this be simplified at all...?
            "dew_point_temperature" => {
                let find_air_temperature = vector.iter().find(|v| v.paramid == 211);
                let find_relative_humidity = vector.iter().find(|v| v.paramid == 262);
                // see if we have the two values
                let data_pair = unwrap_data_pair(find_air_temperature, find_relative_humidity)
                    .map_err(error::internal_error)?;
                if let Some((air_temperature, relative_humidity)) = data_pair {
                    let value = dew_point_temperature(air_temperature, relative_humidity).unwrap();
                    response.push(ProductsResponse {
                        name: element_id.clone(),
                        timestamp: time,
                        value,
                        underlying_data: Some(
                            vec![
                                (
                                    211,
                                    (
                                        air_temperature,
                                        find_air_temperature.and_then(|v| v.quality_code),
                                    ),
                                ),
                                (
                                    262,
                                    (
                                        relative_humidity,
                                        find_relative_humidity.and_then(|v| v.quality_code),
                                    ),
                                ),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                    });
                }
            }
            "mean(water_vapor_partial_pressure_in_air P1D)" => {
                let find_air_temperature = vector.iter().find(|v| v.paramid == 211);
                let find_relative_humidity = vector.iter().find(|v| v.paramid == 262);
                // see if we have the two values
                let data_pair = unwrap_data_pair(find_air_temperature, find_relative_humidity)
                    .map_err(error::internal_error)?;
                if let Some((air_temperature, relative_humidity)) = data_pair {
                    let value =
                        water_vapor_partial_pressure_in_air(air_temperature, relative_humidity)
                            .unwrap();
                    response.push(ProductsResponse {
                        name: element_id.clone(),
                        timestamp: time,
                        value,
                        underlying_data: Some(
                            vec![
                                (
                                    211,
                                    (
                                        air_temperature,
                                        find_air_temperature.and_then(|v| v.quality_code),
                                    ),
                                ),
                                (
                                    262,
                                    (
                                        relative_humidity,
                                        find_relative_humidity.and_then(|v| v.quality_code),
                                    ),
                                ),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                    });
                }
            }
            "specific_humidity" => {
                let find_air_temperature = vector.iter().find(|v| v.paramid == 211);
                let find_relative_humidity = vector.iter().find(|v| v.paramid == 262);
                let find_surface_air_pressure = vector.iter().find(|v| v.paramid == 173);
                // see if we have the two values
                let data_triple = unwrap_data_triple(
                    find_air_temperature,
                    find_relative_humidity,
                    find_surface_air_pressure,
                )
                .map_err(error::internal_error)?;
                if let Some((air_temperature, relative_humidity, surface_air_pressure)) =
                    data_triple
                {
                    let value =
                        specific_humidity(air_temperature, relative_humidity, surface_air_pressure)
                            .unwrap();
                    response.push(ProductsResponse {
                        name: element_id.clone(),
                        timestamp: time,
                        value,
                        underlying_data: Some(
                            vec![
                                (
                                    211,
                                    (
                                        air_temperature,
                                        find_air_temperature.and_then(|v| v.quality_code),
                                    ),
                                ),
                                (
                                    262,
                                    (
                                        relative_humidity,
                                        find_relative_humidity.and_then(|v| v.quality_code),
                                    ),
                                ),
                                (
                                    173,
                                    (
                                        surface_air_pressure,
                                        find_surface_air_pressure.and_then(|v| v.quality_code),
                                    ),
                                ),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                    });
                }
            }
            "over_time(humidity_mixing_ratio P1D)" => {
                let find_air_temperature = vector.iter().find(|v| v.paramid == 211);
                let find_relative_humidity = vector.iter().find(|v| v.paramid == 262);
                let find_surface_air_pressure = vector.iter().find(|v| v.paramid == 173);
                // see if we have the two values
                let data_triple = unwrap_data_triple(
                    find_air_temperature,
                    find_relative_humidity,
                    find_surface_air_pressure,
                )
                .map_err(error::internal_error)?;
                if let Some((air_temperature, relative_humidity, surface_air_pressure)) =
                    data_triple
                {
                    let value = humidity_mixing_ratio(
                        air_temperature,
                        relative_humidity,
                        surface_air_pressure,
                    )
                    .unwrap();
                    response.push(ProductsResponse {
                        name: element_id.clone(),
                        timestamp: time,
                        value,
                        underlying_data: Some(
                            vec![
                                (
                                    211,
                                    (
                                        air_temperature,
                                        find_air_temperature.and_then(|v| v.quality_code),
                                    ),
                                ),
                                (
                                    262,
                                    (
                                        relative_humidity,
                                        find_relative_humidity.and_then(|v| v.quality_code),
                                    ),
                                ),
                                (
                                    173,
                                    (
                                        surface_air_pressure,
                                        find_surface_air_pressure.and_then(|v| v.quality_code),
                                    ),
                                ),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                    });
                }
            }
            _ => error!("No calculations match for element_id: {}", element_id),
        }
    }

    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}

// TODO: figure out how to use the element id to dynamically get the name of the handler?
// or use the element id as a switch in the handler to determine how to calculate the product
pub fn products_router() -> Router<EgressState> {
    Router::new()
        .route("/available/{element_id}", get(products_available_handler))
        .route("/{element_id}", get(products_handler))
}
