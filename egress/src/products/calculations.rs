use crate::error;
use crate::error::Error;
use crate::patchwork;
use crate::patchwork::OpenTimerange;
use crate::patchwork::PatchworkLabel;
use crate::patchwork::PatchworkTables;
use crate::products::ProductsConstructor;
use crate::EgressState;
use crate::ProductTables;
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Timelike, Utc};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::error;
use util::deserialize::comma_separated;
use util::DbPools;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductParams {
    #[serde(deserialize_with = "comma_separated")]
    stationids: Vec<i32>,
    #[serde(deserialize_with = "comma_separated")]
    levels: Vec<i32>,
    #[serde(deserialize_with = "comma_separated")]
    sensors: Vec<i32>,
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

pub fn dew_point_temperature(temperature: f64, relative_humidity: f64) -> Result<f64, Error> {
    let vp = calc_vp(temperature, relative_humidity);
    let td = calc_td(temperature, vp);

    Ok((td * 10.0).round() / 10.0)
}

pub fn calc_vp(ta: f64, uu: f64) -> f64 {
    let e = calc_vp_vapor(ta);
    (uu * e) / 100.0
}

fn calc_vp_vapor(ta: f64) -> f64 {
    // Tetens over 0 and Magnus under 0?
    // possible dirty trick which will look very messy:
    // (ta+abs(ta))/(2*ta) is +1 for pos and 0 for neg
    // (ta-abs(ta))/(2*ta) is 0 for pos and +1 for neg
    if ta > 0.0 {
        return 6.10780 * (17.08085 * ta / (234.175 + ta)).exp();
    }
    6.10780 * (17.84362 * ta / (245.425 + ta)).exp()
}

pub fn calc_td(ta: f64, vp: f64) -> f64 {
    if ta > 0.0 {
        return 245.425 * (vp / 6.10780).ln() / (17.84362 - (vp / 6.10780).ln());
    }
    234.175 * (vp / 6.10780).ln() / (17.08085 - (vp / 6.10780).ln())
}

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

pub fn get_available_products(
    product_tables: ProductTables,
    element: &String,
) -> Result<Vec<ProductsConstructor>, (StatusCode, String)> {
    let product_guard = product_tables.open.read().map_err(error::internal_error)?;

    let available = product_guard.get(element).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("No potential products found for element_id: {}", element),
        )
    })?;
    Ok(available.to_vec())
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

pub async fn products_available_handler(
    Path(element_id): Path<String>,
    State(product_tables): State<ProductTables>,
) -> Result<Json<Vec<ProductsAvailableResponse>>, (StatusCode, String)> {
    // TODO:
    // load list of timeseries that have the inputs for each product, for products "available"
    let available: Vec<ProductsConstructor> = get_available_products(product_tables, &element_id)?;
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

#[axum::debug_handler(state = EgressState)]
pub async fn products_handler(
    State(pools): State<DbPools>,
    Path(element_id): Path<String>,
    State(product_tables): State<ProductTables>,
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<ProductParams>,
) -> Result<Json<Vec<ProductsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let available: Vec<ProductsConstructor> = get_available_products(product_tables, &element_id)?;
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let mut data: HashMap<DateTime<Utc>, Vec<CalculationsDatum>> = HashMap::new();

    // actually get the data from PATCHWORK for each of the input paramids
    for ts in available.iter() {
        // only do for the requested stationid/level/sensor
        // TODO: deal more cleanly with multiple stationids/levels/sensors???
        if !params.stationids.contains(&ts.label.station_id)
            || !params.levels.contains(&ts.label.level.unwrap_or(0))
            || !params.sensors.contains(&ts.label.sensor.unwrap_or(0))
        {
            // does not match requested station/level/sensor
            // skip
            continue;
        }
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
            // TODO: make a massive match statement with all the names???
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
            _ => error!("No calculations match for element_id: {}", element_id),
        }
    }

    // sort by time...
    response.sort_by_key(|p| p.timestamp);
    Ok(Json(response))
}
