use std::collections::HashMap;

use crate::error;
use crate::error::Error;
use crate::products::ProductsConstructor;
use crate::EgressState;
use crate::ProductTables;
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use futures::{stream::FuturesOrdered, StreamExt};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::error;
use util::deserialize::comma_separated;
use util::{DbPools, PooledPgConn};

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
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductsResponse {
    name: String,
    timestamp: DateTime<Utc>,
    value: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CalculationsDatum {
    paramid: i32,
    original: Option<f64>,
    corrected: Option<f64>,
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

pub fn get_available_products(
    product_tables: ProductTables,
    element: &String,
) -> Result<Vec<ProductsConstructor>, (StatusCode, String)> {
    let product_guard = product_tables.open.read().map_err(error::internal_error)?;

    let available = product_guard.get(element).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("No products found for element_id: {}", element),
        )
    })?;
    Ok(available.to_vec())
}

pub async fn get_calculation_data(
    conn: &PooledPgConn<'_>,
    tsids_paramids: Vec<(i64, i32)>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<HashMap<DateTime<Utc>, Vec<CalculationsDatum>>, Error> {
    // TODO: do something with quality code, so have the underlying ts quality for the product
    let query = conn
        .prepare(
            "SELECT timeseries, obstime, original, corrected, quality_code \
                FROM legacy.data \
                WHERE timeseries = $1 \
                    AND obstime >= $2 \
                    AND obstime < $3 \
                ORDER BY obstime",
        )
        .await?;

    let mut futures = tsids_paramids
        .iter()
        .map(async |id| conn.query(&query, &[&id.0, &from, &to]).await)
        .collect::<FuturesOrdered<_>>()
        .enumerate();

    let mut data: HashMap<DateTime<Utc>, Vec<CalculationsDatum>> = HashMap::new();

    while let Some((i, res)) = futures.next().await {
        let rows = match res {
            Ok(val) => val,
            Err(err) => {
                error!("getting last obstime failed: {}, {}", i, err);
                continue;
            }
        };
        for row in rows {
            let tsid: i64 = row.get(0);
            let time: DateTime<Utc> = row.get(1);
            let paramid = tsids_paramids.iter().find(|i| i.0 == tsid);
            if let Some(p) = paramid {
                // put the paramid back with the data for use later
                let datum = CalculationsDatum {
                    paramid: p.1,
                    original: row.get(2),
                    corrected: row.get(3),
                };
                data.entry(time).or_default().push(datum);
            }
        }
    }

    Ok(data)
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
        available_products.push(ProductsAvailableResponse {
            element: element_id.clone(),
            from: product.from,
            to: product.to,
        });
    }

    Ok(Json(available_products))
}

#[axum::debug_handler(state = EgressState)]
pub async fn products_handler(
    State(pools): State<DbPools>,
    Path(element_id): Path<String>,
    State(product_tables): State<ProductTables>,
    Query(params): Query<ProductParams>,
) -> Result<Json<Vec<ProductsResponse>>, (StatusCode, String)> {
    // get the data for the station and time
    let available: Vec<ProductsConstructor> = get_available_products(product_tables, &element_id)?;
    let tsid_paramid_list: Vec<(i64, i32)> =
        available.iter().map(|p| (p.tsid, p.paramid)).collect();

    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    // actually get the data
    let data = get_calculation_data(&open_conn, tsid_paramid_list, params.from, params.to)
        .await
        .map_err(error::internal_error);
    let mut response: Vec<ProductsResponse> = Vec::new();

    if let Ok(d) = data {
        // do the calculation
        for (time, vector) in d {
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
                        let value =
                            dew_point_temperature(air_temperature, relative_humidity).unwrap();
                        response.push(ProductsResponse {
                            name: element_id.clone(),
                            timestamp: time,
                            value,
                        });
                    }
                }
                _ => error!("No calculations match for element_id: {}", element_id),
            }
        }
    }
    // return something for now...
    Ok(Json(response))
}
